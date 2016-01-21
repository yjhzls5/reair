package com.airbnb.di.hive.batchreplication;


import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;

import java.util.Arrays;

public class ExtendedFileStatus {
  private final long fileSize;
  private final long modificationTime;
  private final String[] fileNameParts;

  public ExtendedFileStatus(String path, long fileSize, long modificationTime) {
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
    this.fileNameParts = path.split("/");
    assert this.fileNameParts.length > 3;
  }

  public String getHostName() {
    return fileNameParts[2];
  }

  public String getPath() {
    return "/" + Joiner.on("/").join(Arrays.copyOfRange(fileNameParts, 3, fileNameParts.length));
  }

  public String getFullPath() {
    return Joiner.on("/").join(fileNameParts);
  }

  public String getFileName() {
    return fileNameParts[fileNameParts.length - 1];
  }

  public long getFileSize() {
    return fileSize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("path", Joiner.on("/").join(fileNameParts))
        .add("size", fileSize).add("ts", modificationTime).toString();
  }
}

