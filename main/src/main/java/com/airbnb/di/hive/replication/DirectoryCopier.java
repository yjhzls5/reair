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

public class DirectoryCopier {

  private Configuration conf;
  private Path tmpDir;
  private boolean checkFileModificationTimes;

  /**
   * TODO.
   *
   * @param conf TODO
   * @param tmpDir TODO
   * @param checkFileModificationTimes TODO
   */
  public DirectoryCopier(Configuration conf, Path tmpDir, boolean checkFileModificationTimes) {
    this.conf = conf;
    this.tmpDir = tmpDir;
    this.checkFileModificationTimes = checkFileModificationTimes;
  }

  /**
   * Copy the source directory to the destination directory.
   *
   * @param srcDir TODO
   * @param destDir TODO
   * @param copyAttributes a list of attributes to use when creating the tmp directory. Doesn't
   *        really matter, but it can make it easier to manually inspect the tmp directory.
   * @return TODO
   * @throws IOException TODO
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
   * TODO.
   *
   * @param srcDir TODO
   * @param destDir TODO
   * @return TODO
   *
   * @throws IOException TODO
   */
  public boolean equalDirs(Path srcDir, Path destDir) throws IOException {
    return FsUtils.equalDirs(conf, srcDir, destDir, Optional.empty(), checkFileModificationTimes);
  }
}
