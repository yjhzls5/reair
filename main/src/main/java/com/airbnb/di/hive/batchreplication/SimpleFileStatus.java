package com.airbnb.di.hive.batchreplication;

import com.google.common.base.MoreObjects;

import org.apache.hadoop.fs.Path;

import java.net.URI;

public class SimpleFileStatus {
  private final long fileSize;
  private final long modificationTime;
  private final Path path;

  /**
   * TODO.
   *
   * @param path TODO
   * @param fileSize TODO
   * @param modificationTime TODO
   */
  public SimpleFileStatus(String path, long fileSize, long modificationTime) {
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
    this.path = new Path(path);
  }

  /**
   * TODO.
   *
   * @param path TODO
   * @param fileSize TODO
   * @param modificationTime TODO
   */
  public SimpleFileStatus(Path path, long fileSize, long modificationTime) {
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
    this.path = path;
  }

  public String getPath() {
    return path.toUri().getPath();
  }

  public String getFullPath() {
    return path.toString();
  }

  public String getFileName() {
    return path.getName();
  }

  public long getFileSize() {
    return fileSize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public URI getUri() {
    return path.toUri();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("path", path.toString())
        .add("size", fileSize).add("ts", modificationTime).toString();
  }
}

