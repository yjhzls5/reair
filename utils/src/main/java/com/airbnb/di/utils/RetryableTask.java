package com.airbnb.di.utils;


public interface RetryableTask {
    void run() throws Exception;
}
