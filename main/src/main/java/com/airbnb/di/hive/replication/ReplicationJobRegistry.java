package com.airbnb.di.hive.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Keeps track of a set of jobs
 */
public class ReplicationJobRegistry {

  private static long MAX_RETIRED_JOBS = 200;

  TreeMap<Long, ReplicationJob> idToReplicationJob = new TreeMap<>();

  LinkedList<ReplicationJob> retiredJobs = new LinkedList<>();

  public ReplicationJobRegistry() {}

  synchronized public void registerJob(ReplicationJob job) {
    idToReplicationJob.put(job.getId(), job);
  }

  synchronized public ReplicationJob getJob(long id) {
    return idToReplicationJob.get(id);
  }

  synchronized public ReplicationJob getJobWithSmallestId() {
    if (idToReplicationJob.size() == 0) {
      return null;
    } else {
      return idToReplicationJob.firstEntry().getValue();
    }

  }

  /**
   *
   * @return a collection containing all the active replication jobs. The jobs are returned ordered
   *         by id ascending.
   */
  synchronized public Collection<ReplicationJob> getActiveJobs() {
    return new ArrayList<>(idToReplicationJob.values());
  }

  synchronized public boolean retireJob(ReplicationJob job) {
    ReplicationJob removedJob = idToReplicationJob.remove(job.getId());

    if (removedJob == null) {
      throw new RuntimeException("Couldn't find id: " + job.getId() + " in the registry!");
    }

    if (removedJob != job) {
      throw new RuntimeException("Replication jobs with the same ID " + "are not equal: %s and %s");
    }
    // Trim the size of the list so that we exceed the limit.
    if (retiredJobs.size() + 1 > MAX_RETIRED_JOBS) {
      retiredJobs.remove(0);
    }
    retiredJobs.add(removedJob);
    return true;
  }

  synchronized public Collection<ReplicationJob> getRetiredJobs() {
    return new ArrayList<>(retiredJobs);
  }



}
