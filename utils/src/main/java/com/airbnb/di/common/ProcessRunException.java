package com.airbnb.di.common;

public class ProcessRunException extends Exception {

    public ProcessRunException(String message) {
        super(message);
    }

    public ProcessRunException(Exception e) {
        super(e);
    }

    public ProcessRunException(String message, Exception e) {
        super(message, e);
    }
}
