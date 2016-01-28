package com.airbnb.di.hive.batchreplication.hdfscopy;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

/**
 * InputFormat that scan directories bread first. It will stop at a level when it gets enough
 * splits. The InputSplit it returns will keep track if the folder needs further scan. If it does
 * the recursive scan will be done in RecorderReader. The InputFormat will return file path as key,
 * and file size information as value.
 */
public class DirScanInputFormat extends FileInputFormat<Text, Boolean> {
  private static final Log LOG = LogFactory.getLog(DirScanInputFormat.class);
  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
  private static final int NUMBER_OF_THREADS = 16;
  public static final String NO_FILE_FILTER = "replication.inputformat.nofilter";

  @Override
  public RecordReader<Text, Boolean> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new DirRecordReader();
  }

  private List<FileStatus> getInitialSplits(JobContext job) throws IOException {
    String folderBlackList = job.getConfiguration().get(ReplicationJob.DIRECTORY_BLACKLIST_REGEX);
    boolean nofilter = job.getConfiguration().getBoolean(NO_FILE_FILTER, false);
    ArrayList result = new ArrayList();
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    } else {
      // TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());
      ArrayList errors = new ArrayList();

      for (int i = 0; i < dirs.length; ++i) {
        Path path = dirs[i];
        Configuration conf = job.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] matches = nofilter ? fs.globStatus(path)
                                        : fs.globStatus(path, hiddenFileFilter);
        if (matches == null) {
          errors.add(new IOException("Input path does not exist: " + path));
        } else if (matches.length == 0) {
          errors.add(new IOException("Input Pattern " + path + " matches 0 files"));
        } else {
          for (FileStatus globStat : matches) {
            if (globStat.isDir()) {
              if (folderBlackList == null
                  || !globStat.getPath().getName().matches(folderBlackList)) {
                result.add(globStat);
              }
            }
          }
        }
      }

      if (!errors.isEmpty()) {
        throw new InvalidInputException(errors);
      } else {
        LOG.info("Total input directory to process : " + result.size());
        return result;
      }
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    // split into pieces, fetching the splits in parallel
    ExecutorService executor = Executors.newCachedThreadPool();
    List<InputSplit> splits = new ArrayList<>();
    List<FileStatus> dirToProcess = getInitialSplits(context);
    int level = 0;
    final int numberOfMappers = context.getConfiguration().getInt("mapreduce.job.maps", 500);

    try {
      splits.addAll(Lists.transform(dirToProcess, new Function<FileStatus, DirInputSplit>() {
        @Nullable
        @Override
        public DirInputSplit apply(FileStatus status) {
          return new DirInputSplit(status.getPath().toString(), false);
        }
      }));

      boolean finished = false;
      while (!finished) {
        List<Future<List<FileStatus>>> splitfutures = new ArrayList<Future<List<FileStatus>>>();

        final int foldersPerThread = Math.max(dirToProcess.size() / NUMBER_OF_THREADS, 1);

        for (List<FileStatus> range : Lists.partition(dirToProcess, foldersPerThread)) {
          // for each range, pick a live owner and ask it to compute bite-sized splits
          splitfutures
              .add(executor.submit(new SplitCallable(range, context.getConfiguration(), level)));
        }

        dirToProcess = new ArrayList<>();
        // wait until we have all the results back
        for (Future<List<FileStatus>> futureInputSplits : splitfutures) {
          try {
            dirToProcess.addAll(futureInputSplits.get());
          } catch (Exception e) {
            throw new IOException("Could not get input splits", e);
          }
        }

        // at least explore 3 levels
        if (level > 2 && (dirToProcess.size() == 0
            || splits.size() + dirToProcess.size() > 10 * numberOfMappers)) {
          finished = true;
        }

        final boolean leaf = finished;
        splits.addAll(Lists.transform(dirToProcess, new Function<FileStatus, DirInputSplit>() {
          @Nullable
          @Override
          public DirInputSplit apply(FileStatus status) {
            return new DirInputSplit(status.getPath().toString(), leaf);
          }
        }));

        LOG.info(String.format("Running: folder to process size is %d, split size is %d, ",
            dirToProcess.size(), splits.size()));
        level++;
      }
    } finally {
      executor.shutdownNow();
    }

    assert splits.size() > 0;
    Collections.shuffle(splits, new Random(System.nanoTime()));

    final int foldersPerSplit = Math.max(splits.size() / numberOfMappers, 1);

    return Lists.transform(Lists.partition(splits, foldersPerSplit),
        new Function<List<InputSplit>, InputSplit>() {
          @Override
          public InputSplit apply(@Nullable List<InputSplit> inputSplits) {
            return new ListDirInputSplit(inputSplits);
          }
        });
  }

  /**
   * Get list of folders. Find next level of folders and return.
   */
  class SplitCallable implements Callable<List<FileStatus>> {
    private final Configuration conf;
    private final List<FileStatus> candidates;
    private final int level;
    private final String folderBlackList;
    private final boolean nofilter;

    public SplitCallable(List<FileStatus> candidates, Configuration conf, int level) {
      this.candidates = candidates;
      this.conf = conf;
      this.level = level;
      this.folderBlackList = conf.get(ReplicationJob.DIRECTORY_BLACKLIST_REGEX);
      this.nofilter = conf.getBoolean(NO_FILE_FILTER, false);
    }

    public List<FileStatus> call() throws Exception {
      ArrayList<FileStatus> nextLevel = new ArrayList<FileStatus>();

      for (FileStatus f : candidates) {
        if (!f.isDir()) {
          LOG.error(f.getPath() + " is not a directory");
          continue;
        }
        FileSystem fs = f.getPath().getFileSystem(conf);
        try {
          for (FileStatus child : nofilter ? fs.listStatus(f.getPath())
              : fs.listStatus(f.getPath(), hiddenFileFilter)) {
            if (child.isDir()) {
              if (folderBlackList == null || !child.getPath().getName().matches(folderBlackList)) {
                nextLevel.add(child);
              }
            }
          }
        } catch (FileNotFoundException e) {
          LOG.error(f.getPath() + " removed during operation. Skip...");
        }
      }

      LOG.info("Thread " + Thread.currentThread().getId() + ", level " + level + ":processed "
          + candidates.size() + " folders");

      return nextLevel;
    }
  }
}
