package com.airbnb.di.hive.batchreplication.metastore;

/**
 * Exception thrown when there is an issue with the Hive metastore
 */
public class HiveMetastoreException extends Exception {
    public HiveMetastoreException(Throwable throwable) {
        super(throwable);
    }
}
