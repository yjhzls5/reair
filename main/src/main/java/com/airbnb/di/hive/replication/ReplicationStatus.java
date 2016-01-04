package com.airbnb.di.hive.replication;

public enum ReplicationStatus {
    PENDING,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    NOT_COMPLETABLE,
    ABORTED,
}
