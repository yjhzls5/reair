package com.airbnb.di.common;

public class DistCpException extends Exception {

  public DistCpException(String message) {
    super(message);
  }

  public DistCpException(String message, Throwable cause) {
    super(message, cause);
  }

  public DistCpException(Throwable cause) {
    super(cause);
  }
}
