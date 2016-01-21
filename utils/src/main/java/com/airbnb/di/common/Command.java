package com.airbnb.di.common;


public interface Command<ReturnT, ExceptionT extends Throwable> {
  public ReturnT run() throws ExceptionT;
}
