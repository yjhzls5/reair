package com.airbnb.reair.common;

import org.apache.hadoop.fs.Path;

/**
 * A class to encapsulate various options required for running DistCp.
 */
public class DistCpWrapperOptions {

  // The source directory to copy
  private Path srcDir;
  // The destination directory for the copy
  private Path destDir;
  // Where distcp should temporarily copy files to
  private Path distCpTmpDir;
  // The log directory for the distcp job
  private Path distCpLogDir;
  // If atomic, distCp will copy to a temporary directory first and then
  // do a directory move to the final location
  private boolean atomic = true;
  // If the destination directory exists with different data, can it be
  // deleted?
  private boolean canDeleteDest = true;
  // Whether to set the modification times to be the same for the copied files
  private boolean syncModificationTimes = true;
  // Size number of mappers for the distcp job based on the source directory
  // size and the bytes per mapper
  private long bytesPerMapper = (long) 256e6;
  // If the distCp job runs longer than this many ms, fail the job
  private long distcpJobTimeout = 1800 * 1000;
  // If the input data size is smaller than this many ms, use a local -cp
  // command to copy the files.
  private long localCopyThreshold = (long) 256e6;
  // Poll for the progress of DistCp every N ms
  private long distCpPollInterval = 2500;

  /**
   * Constructor for DistCp options.
   *
   * @param srcDir the source directory to copy from
   * @param destDir the destination directory to copy to
   * @param distCpTmpDir the temporary directory to use when copying
   * @param distCpLogDir the log directory to use when copying
   */
  public DistCpWrapperOptions(Path srcDir, Path destDir, Path distCpTmpDir, Path distCpLogDir) {
    this.srcDir = srcDir;
    this.destDir = destDir;
    this.distCpTmpDir = distCpTmpDir;
    this.distCpLogDir = distCpLogDir;
  }

  public DistCpWrapperOptions setAtomic(boolean atomic) {
    this.atomic = atomic;
    return this;
  }

  public DistCpWrapperOptions setCanDeleteDest(boolean canDeleteDest) {
    this.canDeleteDest = canDeleteDest;
    return this;
  }

  public DistCpWrapperOptions setSyncModificationTimes(boolean syncModificationTimes) {
    this.syncModificationTimes = syncModificationTimes;
    return this;
  }

  public DistCpWrapperOptions setBytesPerMapper(long bytesPerMapper) {
    this.bytesPerMapper = bytesPerMapper;
    return this;
  }

  public DistCpWrapperOptions setDistCpJobTimeout(long distCpJobTimeout) {
    this.distcpJobTimeout = distCpJobTimeout;
    return this;
  }

  public DistCpWrapperOptions setLocalCopyThreshold(long localCopyThreshold) {
    this.localCopyThreshold = localCopyThreshold;
    return this;
  }

  public Path getSrcDir() {
    return srcDir;
  }

  public Path getDestDir() {
    return destDir;
  }

  public Path getDistCpTmpDir() {
    return distCpTmpDir;
  }

  public Path getDistCpLogDir() {
    return distCpLogDir;
  }

  public boolean getAtomic() {
    return atomic;
  }

  public boolean getCanDeleteDest() {
    return canDeleteDest;
  }

  public boolean getSyncModificationTimes() {
    return syncModificationTimes;
  }

  public long getBytesPerMapper() {
    return bytesPerMapper;
  }

  public long getDistcpJobTimeout() {
    return distcpJobTimeout;
  }

  public long getLocalCopyThreshold() {
    return localCopyThreshold;
  }

  public long getDistCpPollInterval() {
    return distCpPollInterval;
  }
}
