package com.airbnb.di.hive.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Periodically prints stats for the replication process to the log
 */
public class StatsTracker {

  private static final Log LOG = LogFactory.getLog(StatsTracker.class);

  // By default print the stats every 10 seconds
  private long PRINT_TIME_INTERVAL = 10 * 1000;

  private ReplicationJobRegistry jobRegistry;
  private Timer timer = new Timer(true);
  private volatile long lastCalculatedLag = 0;

  public StatsTracker(ReplicationJobRegistry jobRegistry) {
    this.jobRegistry = jobRegistry;
    // this.setDaemon(true);
    // this.setName(this.getClass().getSimpleName());
    // timer.se
  }

  public void start() {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        ReplicationJob jobWithSmallestId = jobRegistry.getJobWithSmallestId();
        if (jobWithSmallestId == null) {
          LOG.debug("Oldest ID: N/A Age: 0");
          lastCalculatedLag = 0;
        } else {
          long currentTime = System.currentTimeMillis();
          long createTime = jobWithSmallestId.getCreateTime();
          String age = "N/A";
          if (createTime != 0) {
            age = String.format("%.2f hrs", (currentTime - createTime) / 3600.0 / 1000.0);
            lastCalculatedLag = currentTime - createTime;
          } else {
            lastCalculatedLag = 0;
          }
          LOG.debug(String.format("Oldest ID: %s Age: %s", jobWithSmallestId.getId(), age));
        }
      }
    }, 0, PRINT_TIME_INTERVAL);
  }

  public long getLastCalculatedLag() {
    return lastCalculatedLag;
  }
}
