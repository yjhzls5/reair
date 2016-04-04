package com.airbnb.di.hive.replication;

/**
 * Handler that fires when replication jobs change states.
 */
public interface OnStateChangeHandler {

  /**
   * Method to run when the job starts.
   *
   * @param replicationJob job that started
   */
  public void onStart(ReplicationJob replicationJob);

  /**
   * Method to run when the job completes.
   *
   * @param runInfo information about how the job ran
   * @param replicationJob the job that completed
   */
  public void onComplete(RunInfo runInfo, ReplicationJob replicationJob);
}
