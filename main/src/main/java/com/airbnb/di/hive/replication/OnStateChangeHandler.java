package com.airbnb.di.hive.replication;

/**
 * Handler that fires when replication jobs change states.
 */
public interface OnStateChangeHandler {

  public void onStart(ReplicationJob replicationJob);

  public void onComplete(RunInfo runInfo, ReplicationJob replicationJob);
}
