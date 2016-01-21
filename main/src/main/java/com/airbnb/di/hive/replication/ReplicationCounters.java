package com.airbnb.di.hive.replication;

import java.util.HashMap;
import java.util.Map;

public class ReplicationCounters {
  public enum Type {
    // Tasks that have completed successfully
    SUCCESSFUL_TASKS,
    // Tasks that aren't completable (e.g. missing source table), but are
    // otherwise finished.
    NOT_COMPLETABLE_TASKS,
    // Tasks that were submitted to run.
    EXECUTION_SUBMITTED_TASKS,
    // Tasks that failed to execute. This shouldn't happen in normal
    // operation.
    FAILED_TASKS
  }

  private Map<Type, Long> counters;

  public ReplicationCounters() {
    counters = new HashMap<>();
  }

  public synchronized void incrementCounter(Type type) {
    long currentCount = 0;
    if (counters.get(type) != null) {
      currentCount = counters.get(type);
    }
    counters.put(type, currentCount + 1);
  }

  public synchronized long getCounter(Type type) {
    long currentCount = 0;
    if (counters.get(type) != null) {
      currentCount = counters.get(type);
    }
    return currentCount;
  }

  public synchronized void clear() {
    counters.clear();
  }
}
