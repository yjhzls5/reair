package com.airbnb.di.hive.replication;

/**
 * Created by paul_yang on 7/9/15.
 */
public enum ReplicationStatus {
    PENDING,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    NOT_COMPLETABLE,
}
