package com.airbnb.di.hive.replication;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by paul_yang on 8/6/15.
 */
public interface OnStateChangeHandler {
    public void onStart(ReplicationJob replicationJob);

    // TODO: This shouldn't throw an exception
    public void onComplete(RunInfo runInfo, ReplicationJob replicationJob);
}
