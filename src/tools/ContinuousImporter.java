// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;

import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.stats.StatsCollector;

final class ContinuousImporter {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousImporter.class);

  /** Prints usage and exits with the given retval.  */
  static void usage(final ArgP argp, final int retval) {
    System.err.println("Usage: import directory");
    System.err.print(argp.usage());
    System.err.println("This tool can directly read gzip'ed input files.");
    System.exit(retval);
  }

  public static void main(String[] args) throws IOException {
    ArgP argp = new ArgP();
    argp.addOption("--batch", "BATCH",
                   "Number of files to import in one batch"
                   + " (default: 32).");
    argp.addOption("--stretch", "STRETCH",
                   "Number of files to import before restart"
                   + " (default: 6400).");
    argp.addOption("--prefix", "PREFIX",
                   "Prefix of files to import"
                   + " (default: perf-data).");
    CliOptions.addCommon(argp);
    CliOptions.addAutoMetricFlag(argp);
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, 1);
    } else if (args.length != 1) {
      usage(argp, 2);
    }

    final Integer batchSize = Integer.parseInt(argp.get("--batch", "32"));
    final Integer stretchSize = Integer.parseInt(argp.get("--stretch", "6400"));
    final String prefix = argp.get("--prefix", "perf-data");
    final File importDirectory = new File(args[0]);
    final FileWriter statWriter = new FileWriter(new File(importDirectory, "opentsdb_import.stat"), true);

    final HBaseClient client = CliOptions.clientFromOptions(argp);
    // Flush more frequently since we read very fast from the files.
    client.setFlushInterval((short) 500);  // ms
    final TSDB tsdb = new TSDB(client, argp.get("--table", "tsdb"),
                               argp.get("--uidtable", "tsdb-uid"));
    //argp = null;
    try {
      int fileCount = 0;
      while (fileCount < stretchSize) {
        File[] files = listFiles(importDirectory, prefix, batchSize);
        if (files == null) {
          LOG.info("Asked to stop, exiting...");
          try {
            tsdb.shutdown().joinUninterruptibly();
          } catch (Exception e) {
            LOG.error("Unexpected exception", e);
          }
          System.exit(10);
        } else if (files.length == 0) {
          try { Thread.sleep(1000); } catch (InterruptedException e) { throw new RuntimeException("interrupted", e); }
          continue;
        }
        int points = 0;
        final long start_time = System.nanoTime();
        for (final File file : files) {
          points += importFile(client, tsdb, file);
        }
        final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
        LOG.info(String.format("Total: imported %d data points in %.3fs"
                               + " (%.1f points/s)",
                               points, time_delta, (points / time_delta)));
        // TODO(tsuna): Figure out something better than just writing to stderr.
        tsdb.collectStats(new StatsCollector("tsd") {
          @Override
          public final void emit(final String line) {
            System.err.print(line);
          }
        });
        statWriter.write(String.format("Points: %d %.3f %.3f\n", points, System.currentTimeMillis() / 1000.0, time_delta));
        statWriter.flush();
        fileCount += files.length;
        for (final File file : files) {
          file.delete();
        }
      }
    } finally {
      try {
        tsdb.shutdown().joinUninterruptibly();
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        System.exit(1);
      }
    }
  }

  private static final class FileFilter implements FilenameFilter {
    private final String prefix;
    public FileFilter(final String prefix) {
      this.prefix = prefix;
    }
    public boolean accept(File dir, String name) {
      return name.startsWith(prefix) || name.equals("STOP");
    }
  }

  private static File[] listFiles(final File dir, final String prefix, final Integer max) {
    FileFilter filter = new FileFilter(prefix);
    File[] filteredFiles = dir.listFiles(filter);
    for (File file : filteredFiles) {
        if (file.getName().equals("STOP")) return null;
    }
    Arrays.sort(filteredFiles, new Comparator<File>() {
      public int compare(File f1, File f2) {
        return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
      }
    });
    ArrayList<File> fileList = new ArrayList<File>();
    int count = 0;
    while (count < max && count < filteredFiles.length) {
      fileList.add(filteredFiles[count]);
      count += 1;
    }
    return fileList.toArray(new File[] {});
  }

  static volatile boolean throttle = false;

  private static int importFile(final HBaseClient client,
                                final TSDB tsdb,
                                final File file) throws IOException {
    final long start_time = System.nanoTime();
    long ping_start_time = start_time;
    final BufferedReader in = open(file);
    String line = null;
    int points = 0;
    try {
      final class Errback implements Callback<Object, Exception> {
        public Object call(final Exception arg) {
          if (arg instanceof PleaseThrottleException) {
            final PleaseThrottleException e = (PleaseThrottleException) arg;
            LOG.warn("Need to throttle, HBase isn't keeping up.", e);
            throttle = true;
            final HBaseRpc rpc = e.getFailedRpc();
            if (rpc instanceof PutRequest) {
              client.put((PutRequest) rpc);  // Don't lose edits.
            }
            return null;
          }
          LOG.error("Exception caught while processing file "
                    + file.getName(), arg);
          System.exit(2);
          return arg;
        }
        public String toString() {
          return "importFile errback";
        }
      };
      final Errback errback = new Errback();
      while ((line = in.readLine()) != null) {
        final String[] words = Tags.splitString(line, ' ');
        final String metric = words[0];
        if (metric.length() <= 0) {
          throw new RuntimeException("invalid metric: " + metric);
        }
        final long timestamp = Tags.parseLong(words[1]);
        if (timestamp <= 0) {
          throw new RuntimeException("invalid timestamp: " + timestamp);
        }
        final String value = words[2];
        if (value.length() <= 0) {
          throw new RuntimeException("invalid value: " + value);
        }
        final HashMap<String, String> tags = new HashMap<String, String>();
        for (int i = 3; i < words.length; i++) {
          if (!words[i].isEmpty()) {
            Tags.parse(tags, words[i]);
          }
        }
        final String key = metric + tags;
        if (!checkAndSetTimestamp(key, timestamp)) continue;
        final WritableDataPoints dp = getDataPoints(tsdb, key, metric, tags);
        Deferred<Object> d;
        if (value.indexOf('.') < 0) {  // integer value
          d = dp.addPoint(timestamp, Tags.parseLong(value));
        } else {  // floating point value
          d = dp.addPoint(timestamp, Float.parseFloat(value));
        }
        d.addErrback(errback);
        points++;
        if (points % 1000000 == 0) {
          final long now = System.nanoTime();
          ping_start_time = (now - ping_start_time) / 1000000;
          LOG.info(String.format("... %d data points in %dms (%.1f points/s)",
                                 points, ping_start_time,
                                 (1000000 * 1000.0 / ping_start_time)));
          ping_start_time = now;
        }
        if (throttle) {
          LOG.info("Throttling...");
          long throttle_time = System.nanoTime();
          try {
            d.joinUninterruptibly();
          } catch (Exception e) {
            throw new RuntimeException("Should never happen", e);
          }
          throttle_time = System.nanoTime() - throttle_time;
          if (throttle_time < 1000000000L) {
            LOG.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
            try { Thread.sleep(1000); } catch (InterruptedException e) { throw new RuntimeException("interrupted", e); }
          }
          LOG.info("Done throttling...");
          throttle = false;
        }
      }
    } catch (RuntimeException e) {
        LOG.error("Exception caught while processing file "
                  + file.getName() + " line=" + line);
        throw e;
    } finally {
      in.close();
    }
    final long time_delta = (System.nanoTime() - start_time) / 1000000;
    LOG.info(String.format("Processed %s in %d ms, %d data points"
                           + " (%.1f points/s)",
                           file.getName(), time_delta, points,
                           (points * 1000.0 / time_delta)));
    return points;
  }

  /**
   * Opens a file for reading, handling gzipped files.
   * @param file The file to open.
   * @return A buffered reader to read the file, decompressing it if needed.
   * @throws IOException when shit happens.
   */
  private static BufferedReader open(final File file) throws IOException {
    InputStream is = new FileInputStream(file);
    if (file.getName().endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    // I <3 Java's IO library.
    return new BufferedReader(new InputStreamReader(is));
  }

  private static final HashMap<String, WritableDataPoints> datapoints =
    new HashMap<String, WritableDataPoints>();

  private static
    WritableDataPoints getDataPoints(final TSDB tsdb,
                                     final String key,
                                     final String metric,
                                     final HashMap<String, String> tags) {
    WritableDataPoints dp = datapoints.get(key);
    if (dp != null) {
      return dp;
    }
    dp = tsdb.newDataPoints();
    dp.setSeries(metric, tags);
    dp.setBatchImport(true);
    datapoints.put(key, dp);
    return dp;
  }

  private static final HashMap<String, Long> lastTimestamps =
    new HashMap<String, Long>();

  private static boolean checkAndSetTimestamp(final String key, final Long t) {
    Long ct = lastTimestamps.get(key);
    boolean r = ct == null ? true : t > ct;
    if (r) lastTimestamps.put(key, t);
    return r;
  }

}
