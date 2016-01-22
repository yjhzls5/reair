package com.airbnb.di.common;

public class ProcessRunException extends Exception {

  public ProcessRunException(String message) {
    super(message);
  }

  public ProcessRunException(Exception exception) {
    super(exception);
  }

  public ProcessRunException(String message, Exception exception) {
    super(message, exception);
  }
}
