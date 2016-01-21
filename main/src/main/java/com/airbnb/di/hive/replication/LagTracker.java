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

  synchronized public void add(ReplicationJob job) {
    if (idToCreateTime.containsKey(job.getId())) {
      throw new RuntimeException(String.format("Key: %s already exists!", job.getId()));
    }

    Optional<String> createTime = Optional.ofNullable(job.getPersistedJobInfo().getExtras()
        .get(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY));

    idToCreateTime.put(job.getId(), createTime.map(Long::parseLong).orElse(Long.valueOf(0)));
  }

  synchronized public void remove(ReplicationJob job) {
    if (!idToCreateTime.containsKey(job.getId())) {
      throw new RuntimeException(String.format("Unknown key: %s", job.getId()));
    }

    idToCreateTime.remove(job.getId());
  }

  synchronized public Long getAgeOfOldestEntry() {
    return getAgeOfOldestEntry(System.currentTimeMillis());
  }

  synchronized public Long getAgeOfOldestEntry(long currentTime) {
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

  synchronized public Long getIdOfOldestEntry() {

    if (idToCreateTime.size() == 0) {
      return null;
    }

    return idToCreateTime.firstKey();
  }

  synchronized public Long getCreteTimeOfOldestEntry() {
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
