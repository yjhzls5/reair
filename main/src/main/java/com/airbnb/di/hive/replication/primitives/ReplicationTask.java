package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.multiprocessing.LockSet;

import java.io.IOException;

/**
 * Interface for a replication task. A replication task is one of many primitives that can be used
 * to replicate data and actions from the source warehouse to the destination warehouse.
 */
public interface ReplicationTask {
  /**
   * Runs the replication task without retries.
   *
   * @return RunInfo containing how the task execution went
   * @throws HiveMetastoreException if there is an error making a metastore call
   * @throws IOException if there is an error with writing to files
   * @throws DistCpException if there is an error running DistCp
   */
  public RunInfo runTask() throws HiveMetastoreException, IOException, DistCpException;

  /**
   * To handle concurrency issues, replication tasks should specify a set of locks so that two
   * conflicting replication tasks do not run at the same time.
   *
   * @return a set of locks that this task should acquire before running
   */
  public LockSet getRequiredLocks();
}
