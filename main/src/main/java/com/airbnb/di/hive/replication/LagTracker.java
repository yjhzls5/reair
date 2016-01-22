package com.airbnb.di.hive.replication;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class LagTracker {
  private TreeMap<Long, Long> idToCreateTime = new TreeMap<Long, Long>();

  class OldestEntryInfo {
    public long id;
    public long createTime;
    public long age;
  }

  /**
   * TODO.
   *
   * @param job TODO
   */
  public synchronized void add(ReplicationJob job) {
    if (idToCreateTime.containsKey(job.getId())) {
      throw new RuntimeException(String.format("Key: %s already exists!", job.getId()));
    }

    Optional<String> createTime = Optional.ofNullable(job.getPersistedJobInfo().getExtras()
        .get(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY));

    idToCreateTime.put(job.getId(), createTime.map(Long::parseLong).orElse(Long.valueOf(0)));
  }

  /**
   * TODO.
   *
   * @param job TODO
   */
  public synchronized void remove(ReplicationJob job) {
    if (!idToCreateTime.containsKey(job.getId())) {
      throw new RuntimeException(String.format("Unknown key: %s", job.getId()));
    }

    idToCreateTime.remove(job.getId());
  }

  /**
   * TODO.
   *
   * @return TODO
   */
  public synchronized Long getAgeOfOldestEntry() {
    return getAgeOfOldestEntry(System.currentTimeMillis());
  }

  /**
   * TODO.
   *
   * @param currentTime TODO
   * @return TODO
   */
  public synchronized Long getAgeOfOldestEntry(long currentTime) {
    if (idToCreateTime.size() == 0) {
      return null;
    }
    Long firstKey = idToCreateTime.firstKey();
    if (firstKey == null) {
      return null;
    } else {
      return currentTime - idToCreateTime.get(firstKey).longValue();
    }
  }

  /**
   * TODO.
   *
   * @return TODO
   */
  public synchronized Long getIdOfOldestEntry() {
    if (idToCreateTime.size() == 0) {
      return null;
    }

    return idToCreateTime.firstKey();
  }

  /**
   * TODO.
   *
   * @return TODO
   */
  public synchronized Long getCreteTimeOfOldestEntry() {
    if (idToCreateTime.size() == 0) {
      return null;
    }

    Long key = idToCreateTime.firstKey();

    if (key == null) {
      return null;
    }
    return idToCreateTime.get(key);
  }
}
