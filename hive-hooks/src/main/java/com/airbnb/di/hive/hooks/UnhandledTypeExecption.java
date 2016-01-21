package com.airbnb.di.hive.hooks;

public class UnhandledTypeExecption extends Exception {
  public UnhandledTypeExecption() {}

  public UnhandledTypeExecption(String message) {
    super(message);
  }

  public UnhandledTypeExecption(String message, Throwable cause) {
    super(message, cause);
  }

  public UnhandledTypeExecption(Throwable cause) {
    super(cause);
  }
}
