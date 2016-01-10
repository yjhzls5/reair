package com.airbnb.di.hive.replication;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.primitives.ReplicationTask;
import com.airbnb.di.multiprocessing.Job;
import com.airbnb.di.multiprocessing.LockSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ReplicationJob extends Job {
    private static final Log LOG = LogFactory.getLog(
            ReplicationJob.class);

    // The number of ms to sleep between retries if the replication task fails
    private static long RETRY_SLEEP_TIME_MS = 60 * 1000;

    private ReplicationTask replicationTask;
    private OnStateChangeHandler onStateChangeHandler;
    private PersistedJobInfo persistedJobInfo;

    public ReplicationJob(ReplicationTask replicationTask,
                          OnStateChangeHandler onStateChangeHandler,
                          PersistedJobInfo persistedJobInfo) {
        this.replicationTask = replicationTask;
        this.onStateChangeHandler = onStateChangeHandler;
        this.persistedJobInfo = persistedJobInfo;
    }

    public PersistedJobInfo getPersistedJobInfo() {
        return persistedJobInfo;
    }

    @Override
    public int run() {
        int attempt = 0;
        while (true) {
            try {
                onStateChangeHandler.onStart(this);
                RunInfo runInfo = replicationTask.runTask();
                LOG.info(String.format("Replication job id: %s finished " +
                        "with status %s",
                        persistedJobInfo.getId(),
                        runInfo.getRunStatus()));
                onStateChangeHandler.onComplete(runInfo, this);

                switch (runInfo.getRunStatus()) {
                    case SUCCESSFUL:
                    case NOT_COMPLETABLE:
                        return 0;
                    case FAILED:
                        return -1;
                    default:
                        throw new RuntimeException("State not handled: " +
                                runInfo.getRunStatus());
                }
            } catch (HiveMetastoreException e) {
                LOG.error("Got an exception - will retry", e);
            } catch (IOException e) {
                LOG.error("Got an exception - will retry", e);
            } catch (DistCpException e) {
                LOG.error("Got an exception - will retry", e);
            }
            LOG.error("Because job id: " + getId() + " was not successful, " +
                    "it will be retried after sleeping.");

            try {
                ReplicationUtils.exponentialSleep(attempt);
            } catch (InterruptedException e) {
                LOG.warn("Got interrupted", e);
                return -1;
            }

            attempt++;
        }
    }

    @Override
    public LockSet getRequiredLocks() {
        return replicationTask.getRequiredLocks();
    }

    @Override
    public String toString() {
        return "ReplicationJob{" +
                "persistedJobInfo=" + persistedJobInfo +
                '}';
    }

    public long getId() {
        return persistedJobInfo.getId();
    }

    // TODO: Does this belong in this class?

    public long getCreateTime() {
        Optional<String> createTime = Optional.ofNullable(getPersistedJobInfo()
                .getExtras()
                .get(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY));

        return createTime.map(Long::parseLong).orElse(Long.valueOf(0));
    }

    public Collection<Long> getParentJobIds() {
        // TODO: Casting is no bueno
        Set<Job> parentJobs = getParentJobs();
        List<Long> parentJobIds = new ArrayList<Long>();

        for (Job parentJob : parentJobs) {
            ReplicationJob replicationJob = (ReplicationJob)parentJob;
            parentJobIds.add(replicationJob.getId());
        }

        return parentJobIds;
    }
}
