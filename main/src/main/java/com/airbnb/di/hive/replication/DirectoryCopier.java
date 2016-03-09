package com.airbnb.di.hive.replication;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.common.DistCpWrapper;
import com.airbnb.di.common.DistCpWrapperOptions;
import com.airbnb.di.common.FsUtils;
import com.airbnb.di.common.PathBuilder;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * Copies directories on Hadoop filesystems.
 */
public class DirectoryCopier {

  private Configuration conf;
  private Path tmpDir;
  private boolean checkFileModificationTimes;

  /**
   * Constructor for the directory copier.
   *
   * @param conf configuration object
   * @param tmpDir the temporary directory to copy data to before moving to the final destination
   * @param checkFileModificationTimes Whether to check that the modified times of the files match
   *                                   after the copy. Some filesystems do not support preservation
   *                                   of modified file time after a copy, so this check may need to
   *                                   be disabled.
   */
  public DirectoryCopier(Configuration conf, Path tmpDir, boolean checkFileModificationTimes) {
    this.conf = conf;
    this.tmpDir = tmpDir;
    this.checkFileModificationTimes = checkFileModificationTimes;
  }

  /**
   * Copy the source directory to the destination directory.
   *
   * @param srcDir source directory
   * @param destDir destination directory
   * @param copyAttributes a list of attributes to use when creating the tmp directory. Doesn't
   *        really matter, but it can make it easier to manually inspect the tmp directory.
   * @return the number of bytes copied
   * @throws IOException if there was an error copying the directory
   */
  public long copy(Path srcDir, Path destDir, List<String> copyAttributes) throws IOException {
    Random random = new Random();
    long randomLong = Math.abs(random.nextLong());

    PathBuilder tmpDirPathBuilder = new PathBuilder(tmpDir).add("distcp_tmp");
    for (String attribute : copyAttributes) {
      tmpDirPathBuilder.add(attribute);
    }
    Path distCpTmpDir = tmpDirPathBuilder.add(Long.toHexString(randomLong)).toPath();

    PathBuilder logDirPathBuilder = new PathBuilder(tmpDir).add("distcp_logs");

    for (String attribute : copyAttributes) {
      logDirPathBuilder.add(attribute);
    }

    Path distCpLogDir = logDirPathBuilder.add(Long.toHexString(randomLong)).toPath();

    try {
      // Copy directory
      DistCpWrapper distCpWrapper = new DistCpWrapper(conf);
      DistCpWrapperOptions options =
          new DistCpWrapperOptions(srcDir, destDir, distCpTmpDir, distCpLogDir)
              .setAtomic(true)
              .setSyncModificationTimes(checkFileModificationTimes);

      long copyJobTimeoutSeconds = conf.getLong(
          DeployConfigurationKeys.COPY_JOB_TIMEOUT_SECONDS,
          -1);
      if (copyJobTimeoutSeconds > 0) {
        options.setDistCpJobTimeout(copyJobTimeoutSeconds * 1000);
      }

      long bytesCopied = distCpWrapper.copy(options);
      return bytesCopied;
    } catch (DistCpException e) {
      throw new IOException(e);
    }
  }

  /**
   * Checks to see if two directories contain the same files. Same is defined as having the same set
   * of non-empty files with matching file sizes (and matching modified times if set in the
   * constructor)
   *
   * @param srcDir source directory
   * @param destDir destination directory
   * @return TODO
   *
   * @throws IOException TODO
   */
  public boolean equalDirs(Path srcDir, Path destDir) throws IOException {
    return FsUtils.equalDirs(conf, srcDir, destDir, Optional.empty(), checkFileModificationTimes);
  }
}
