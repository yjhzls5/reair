package com.airbnb.di.hive.replication;

public class MetadataException extends Exception {
  public MetadataException(String message) {
    super(message);
  }

  public MetadataException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetadataException(Throwable cause) {
    super(cause);
  }
}
