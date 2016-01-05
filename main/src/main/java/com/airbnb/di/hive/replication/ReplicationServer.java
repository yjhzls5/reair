package com.airbnb.di.hive.replication;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;
import com.airbnb.di.hive.replication.auditlog.AuditLogReader;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.filter.ReplicationFilter;
import com.airbnb.di.db.DbKeyValueStore;
import com.airbnb.di.hive.replication.primitives.CopyPartitionTask;
import com.airbnb.di.hive.replication.primitives.CopyPartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.CopyPartitionsTask;
import com.airbnb.di.hive.replication.primitives.CopyUnpartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.DropPartitionTask;
import com.airbnb.di.hive.replication.primitives.DropTableTask;
import com.airbnb.di.hive.replication.primitives.RenameTableTask;
import com.airbnb.di.hive.replication.primitives.ReplicationTask;
import com.airbnb.di.hive.replication.thrift.TReplicationJob;
import com.airbnb.di.hive.replication.thrift.TReplicationService;
import com.airbnb.di.multiprocessing.ParallelJobExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ReplicationServer implements TReplicationService.Iface {

    private static final Log LOG = LogFactory.getLog(
            ReplicationServer.class);

    // If there is a need to wait to poll, wait this many ms
    private final long POLL_WAIT_TIME_MS = 10 * 1000;

    // Key used for storing the last persisted audit log ID in the key value
    // store
    private final String LAST_PERSISTED_AUDIT_LOG_ID_KEY = "last_persisted_id";

    private Configuration conf;
    private Cluster srcCluster;
    private Cluster destCluster;

    private OnStateChangeHandler onStateChangeHandler;
    private ObjectConflictHandler objectConflictHandler;
    private DestinationObjectFactory destinationObjectFactory;

    private AuditLogReader auditLogReader;
    private DbKeyValueStore keyValueStore;
    private final PersistedJobInfoStore jobInfoStore;

    private ParallelJobExecutor jobExecutor;
    private ParallelJobExecutor copyPartitionJobExecutor;

    private ReplicationFilter replicationFilter;

    // Collect stats with counters
    private ReplicationCounters counters = new ReplicationCounters();

    private ReplicationJobRegistry jobRegistry = new ReplicationJobRegistry();

    private ReplicationJobFactory jobFactory;

    // If the number of jobs that we track in memory exceed this amount, then
    // pause until more jobs finish.
    private int maxJobsInMemory;

    private volatile boolean pauseRequested = false;

    private StatsTracker statsTracker = new StatsTracker(jobRegistry);

    private DirectoryCopier directoryCopier;

    private Long startAfterAuditLogId = null;

    // Responsible for persisting changes to the state of the replication job
    // once it finishes
    private class JobStateChangeHandler implements OnStateChangeHandler {
        @Override
        public void onStart(ReplicationJob replicationJob) {
            // TODO: This is not yet used
            LOG.info("Job id: " + replicationJob.getId() + " started");
            jobInfoStore.changeStautsAndPersist(
                    ReplicationStatus.RUNNING,
                    replicationJob.getPersistedJobInfo());
        }

        @Override
        public void onComplete(RunInfo runInfo,
                               ReplicationJob replicationJob) {
            LOG.info("Job id: " + replicationJob.getId() + " finished " +
                    "with state " + runInfo.getRunStatus() + " and " +
                    runInfo.getBytesCopied() + " bytes copied");

            replicationJob.getPersistedJobInfo().getExtras().put(
                    PersistedJobInfo.BYTES_COPIED_KEY,
                    Long.toString(runInfo.getBytesCopied()));


            LOG.info("Persisting job id: " +
                    replicationJob.getPersistedJobInfo().getId());

            switch (runInfo.getRunStatus()) {
                case SUCCESSFUL:
                    jobInfoStore.changeStautsAndPersist(
                            ReplicationStatus.SUCCESSFUL,
                            replicationJob.getPersistedJobInfo());
                    counters.incrementCounter(
                            ReplicationCounters.Type.SUCCESSFUL_TASKS);
                    break;
                case NOT_COMPLETABLE:
                    jobInfoStore.changeStautsAndPersist(
                            ReplicationStatus.NOT_COMPLETABLE,
                            replicationJob.getPersistedJobInfo());
                    counters.incrementCounter(
                            ReplicationCounters.Type.NOT_COMPLETABLE_TASKS);
                    break;
                case FAILED:
                    jobInfoStore.changeStautsAndPersist(
                            ReplicationStatus.FAILED,
                            replicationJob.getPersistedJobInfo());
                    counters.incrementCounter(
                            ReplicationCounters.Type.FAILED_TASKS);
                    break;
                default:
                    throw new RuntimeException("Unhandled status: " +
                            runInfo.getRunStatus());

            }
            LOG.info("Persisted job: " + replicationJob);
            jobRegistry.retireJob(replicationJob);
        }
    }

    public ReplicationServer(Configuration conf,
                             Cluster srcCluster,
                             Cluster destCluster,
                             AuditLogReader auditLogReader,
                             DbKeyValueStore keyValueStore,
                             final PersistedJobInfoStore jobInfoStore,
                             ReplicationFilter replicationFilter,
                             DirectoryCopier directoryCopier,
                             int numWorkers,
                             int maxJobsInMemory,
                             Long startAfterAuditLogId) {
        this.conf = conf;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.auditLogReader = auditLogReader;
        this.keyValueStore = keyValueStore;
        this.jobInfoStore = jobInfoStore;

        this.onStateChangeHandler = new JobStateChangeHandler();

        this.objectConflictHandler = new ObjectConflictHandler();
        this.destinationObjectFactory = new DestinationObjectFactory();

        this.replicationFilter = replicationFilter;

        this.maxJobsInMemory = maxJobsInMemory;

        this.jobExecutor = new ParallelJobExecutor(
                "TaskWorker", numWorkers);
        this.copyPartitionJobExecutor = new ParallelJobExecutor(
                "CopyPartitionWorker",
                numWorkers);

        this.directoryCopier = directoryCopier;

        this.jobFactory = new ReplicationJobFactory(conf,
                srcCluster,
                destCluster,
                jobInfoStore,
                destinationObjectFactory,
                onStateChangeHandler,
                objectConflictHandler,
                copyPartitionJobExecutor,
                directoryCopier);

        this.startAfterAuditLogId = startAfterAuditLogId;

        jobExecutor.start();
        copyPartitionJobExecutor.start();


    }

    private ReplicationJob restoreReplicationJob(
            PersistedJobInfo persistedJobInfo) {
        ReplicationTask replicationTask = null;

        HiveObjectSpec tableSpec = new HiveObjectSpec(
                persistedJobInfo.getSrcDbName(),
                persistedJobInfo.getSrcTableName());
        HiveObjectSpec partitionSpec = null;

        if (persistedJobInfo.getSrcPartitionNames() != null &&
                persistedJobInfo.getSrcPartitionNames().size() > 0) {
            partitionSpec = new HiveObjectSpec(
                    persistedJobInfo.getSrcDbName(),
                    persistedJobInfo.getSrcTableName(),
                    persistedJobInfo.getSrcPartitionNames().get(0));
        }
        switch(persistedJobInfo.getOperation()) {
            case COPY_UNPARTITIONED_TABLE:
                replicationTask = new CopyUnpartitionedTableTask(
                        conf,
                        destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        tableSpec,
                        persistedJobInfo.getSrcPath(),
                        directoryCopier);
                break;
            case COPY_PARTITIONED_TABLE:
                replicationTask = new CopyPartitionedTableTask(
                        conf,
                        destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        tableSpec,
                        persistedJobInfo.getSrcPath());
                break;
            case COPY_PARTITION:
                replicationTask = new CopyPartitionTask(
                        conf,
                        destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        partitionSpec,
                        persistedJobInfo.getSrcPath(),
                        null,
                        directoryCopier);
                break;
            case COPY_PARTITIONS:
                List<String> partitionNames =
                        persistedJobInfo.getSrcPartitionNames();
                replicationTask = new CopyPartitionsTask(
                    conf,
                    destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        tableSpec,
                        partitionNames,
                        persistedJobInfo.getSrcPath(),
                        copyPartitionJobExecutor,
                        directoryCopier);
                break;
            case DROP_TABLE:
                replicationTask = new DropTableTask(
                        srcCluster,
                        destCluster,
                        tableSpec,
                        persistedJobInfo.getSrcObjectTldt());
                break;
            case DROP_PARTITION:
                 replicationTask = new DropPartitionTask(
                         srcCluster,
                         destCluster,
                         partitionSpec,
                         persistedJobInfo.getSrcObjectTldt());
                break;
            case RENAME_TABLE:
                replicationTask = new RenameTableTask(
                        conf,
                        srcCluster,
                        destCluster,
                        destinationObjectFactory,
                        objectConflictHandler,
                        tableSpec,
                        new HiveObjectSpec(
                                persistedJobInfo.getRenameToDb(),
                                persistedJobInfo.getRenameToTable()),
                        persistedJobInfo.getSrcPath(),
                        persistedJobInfo.getRenameToPath(),
                        persistedJobInfo.getSrcObjectTldt(),
                        copyPartitionJobExecutor,
                        directoryCopier);
                break;
            case RENAME_PARTITION:
                // TODO: Handle rename partition
            default:
                throw new RuntimeException("Unhandled operation: " +
                        persistedJobInfo.getOperation());
        }

        return new ReplicationJob(replicationTask, onStateChangeHandler,
                persistedJobInfo);
    }

    public void queueJobForExecution(ReplicationJob job) {
        jobExecutor.add(job);
        counters.incrementCounter(
                ReplicationCounters.Type.EXECUTION_SUBMITTED_TASKS);
    }

    public void run(long jobsToComplete)
            throws IOException, SQLException {

        // Clear the counters so that we can accurate stats for this run
        clearCounters();

        // Configure the audit log reader based what's specified, or what was
        // last persisted.
        long lastPersistedAuditLogId = 0;

        if (startAfterAuditLogId != null) {
            // The starting ID was specified
            lastPersistedAuditLogId = startAfterAuditLogId;
        } else {
            // Otherwise, start from the previous stop point
            LOG.info("Fetching last persisted audit log ID");
            String lastPersistedIdString = keyValueStore.get(
                    LAST_PERSISTED_AUDIT_LOG_ID_KEY);

            if (lastPersistedIdString == null) {
                Long maxId = auditLogReader.getMaxId();
                if (maxId == null) {
                    maxId = Long.valueOf(0);
                }
                LOG.warn(String.format("Since the last persisted ID was not previously set, " +
                        "using max ID in the audit log: %s", maxId));
                lastPersistedAuditLogId = maxId;
            } else {
                lastPersistedAuditLogId = Long.parseLong(lastPersistedIdString);
            }
        }

        LOG.info("Using last persisted ID of " + lastPersistedAuditLogId);
        auditLogReader.setReadAfterId(lastPersistedAuditLogId);

        // Resume jobs that were persisted, but were not run.
        for(PersistedJobInfo jobInfo : jobInfoStore.getRunnableFromDb()) {
            LOG.info(String.format("Restoring %s to (re)run", jobInfo));
            ReplicationJob job = restoreReplicationJob(jobInfo);
            jobRegistry.registerJob(job);
            queueJobForExecution(job);
        }

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);

        statsTracker.start();

        // This is the time that the last persisted id was updated in the store.
        // It's tracked to rate limit the number of updates that are done.
        long updateTimeForLastPersistedId = 0;

        while (true) {
            if (pauseRequested) {
                LOG.info("Pause requested. Sleeping...");
                ReplicationUtils.sleep(POLL_WAIT_TIME_MS);
                continue;
            }

            // Stop if we've had enough successful jobs - for testing purposes
            // only
            long completedJobs =
                    counters.getCounter(
                            ReplicationCounters.Type.SUCCESSFUL_TASKS) +
                    counters.getCounter(
                            ReplicationCounters.Type.NOT_COMPLETABLE_TASKS);

            if (jobsToComplete > 0 && completedJobs >= jobsToComplete) {
                LOG.info(String.format("Hit the limit for the number of " +
                "successful jobs (%d) - returning.", jobsToComplete));
                return;
            }

            //  Wait if there are too many jobs
            if (jobExecutor.getNotDoneJobCount() > maxJobsInMemory) {
                LOG.info(String.format("There are too many jobs in memory. " +
                        "Waiting until more complete. (limit: %d)",
                        maxJobsInMemory));
                ReplicationUtils.sleep(POLL_WAIT_TIME_MS);
                continue;
            }

            // Get an entry from the audit log
            LOG.info("Fetching the next entry from the audit log");
            AuditLogEntry auditLogEntry = auditLogReader.resilientNext();

            // If there's nothing from the audit log, then wait for a little bit
            // and then try again.
            if (auditLogEntry == null) {
                LOG.info(String.format("No more entries from the audit log. " +
                        "Sleeping for %s ms", POLL_WAIT_TIME_MS));
                ReplicationUtils.sleep(POLL_WAIT_TIME_MS);
                continue;
            }

            LOG.info("Got audit log entry: " + auditLogEntry);

            // Convert the audit log entry into a replication job, which has
            // elements persisted to the DB
            List<ReplicationJob> replicationJobs =
                    jobFactory.createReplicationJobs(auditLogEntry,
                            replicationFilter);

            LOG.info(String.format("Audit log entry id: %s converted to %s",
                    auditLogEntry.getId(),
                    replicationJobs));

            // Add these jobs to the registry
            for (ReplicationJob job : replicationJobs) {
                jobRegistry.registerJob(job);
            }

            // Since the replication job was created and persisted, we can
            // advance the last persisted ID. Update every 10s to reduce db
            if (System.currentTimeMillis() - updateTimeForLastPersistedId >
                    10000) {
                keyValueStore.resilientSet(
                        LAST_PERSISTED_AUDIT_LOG_ID_KEY,
                        Long.toString(auditLogEntry.getId()));
                updateTimeForLastPersistedId = System.currentTimeMillis();
            }

            for (ReplicationJob replicationJob : replicationJobs) {
                LOG.info("Scheduling: " + replicationJob);
                prettyLogStart(replicationJob);
                long tasksSubmittedForExecution = counters.getCounter(
                        ReplicationCounters.Type.EXECUTION_SUBMITTED_TASKS);

                if (tasksSubmittedForExecution >= jobsToComplete) {
                    LOG.warn(String.format("Not submitting %s for execution " +
                                    " due to the limit for the number of " +
                                    "jobs to execute", replicationJob));
                    return;
                } else {
                    queueJobForExecution(replicationJob);
                }
            }
        }
    }

    /**
     * Resets the counters - for testing purposes
     */
    public void clearCounters() {
        counters.clear();
    }

    @Override
    public List<TReplicationJob> getActiveJobs(long afterId, int maxJobs)
            throws TException {
        int count = 0;
        List<TReplicationJob> jobsToReturn = new ArrayList<TReplicationJob>();

        for (ReplicationJob job : jobRegistry.getActiveJobs()) {
            if (count == maxJobs) {
                break;
            }

            if (job.getId() > afterId) {
                count++;
                TReplicationJob tJob = ThriftObjectUtils.convert(job);
                jobsToReturn.add(tJob);
            }
        }
        // TODO: Remove test log line
        LOG.info("getActiveJobs: returning " + jobsToReturn.size() + " jobs");
        return jobsToReturn;
    }

    @Override
    synchronized public void pause() throws TException {
        LOG.info("Paused requested");
        if (pauseRequested) {
            LOG.warn("Server is already paused!");
        } else {
            pauseRequested = true;

            try {
                copyPartitionJobExecutor.stop();
                jobExecutor.stop();
            } catch (InterruptedException e) {
                LOG.error("Unexpected interruption", e);
            }
        }
    }

    @Override
    synchronized public void resume() throws TException {
        LOG.info("Resume requested");
        if (!pauseRequested) {
            LOG.warn("Server is already resumed!");
        } else {
            pauseRequested = false;
            copyPartitionJobExecutor.start();
            jobExecutor.start();
        }
    }

    @Override
    public long getLag() throws TException {
        return statsTracker.getLastCalculatedLag();
    }

    @Override
    public Map<Long, TReplicationJob> getJobs(List<Long> ids) {
        throw new RuntimeException("Not yet implemented!");
    }

    @Override
    public List<TReplicationJob> getRetiredJobs(long afterId, int maxJobs)
            throws TException {
        int count = 0;
        List<TReplicationJob> jobsToReturn = new ArrayList<TReplicationJob>();

        for (ReplicationJob job : jobRegistry.getRetiredJobs()) {
            if (count == maxJobs) {
                break;
            }

            if (job.getId() > afterId) {
                count++;
                TReplicationJob tJob = ThriftObjectUtils.convert(job);
                jobsToReturn.add(tJob);
            }
        }
        // TODO: Remove test log line
        LOG.info("getRetiredJobs: returning " + jobsToReturn.size() + " jobs");
        return jobsToReturn;
    }

    private void prettyLogStart(ReplicationJob job) {
        List<HiveObjectSpec> srcSpecs = new ArrayList<HiveObjectSpec>();

        if (job.getPersistedJobInfo().getSrcPartitionNames() != null &&
                job.getPersistedJobInfo().getSrcPartitionNames().size() > 0) {
            for (String partitionName :
                    job.getPersistedJobInfo().getSrcPartitionNames()) {
                HiveObjectSpec spec = new HiveObjectSpec(
                        job.getPersistedJobInfo().getSrcDbName(),
                        job.getPersistedJobInfo().getSrcTableName(),
                        partitionName);
                srcSpecs.add(spec);
            }
        } else {
            HiveObjectSpec spec = new HiveObjectSpec(
                    job.getPersistedJobInfo().getSrcDbName(),
                    job.getPersistedJobInfo().getSrcTableName());
            srcSpecs.add(spec);
        }

        HiveObjectSpec renameToSpec = new HiveObjectSpec(
                job.getPersistedJobInfo().getRenameToDb(),
                job.getPersistedJobInfo().getRenameToTable(),
                job.getPersistedJobInfo().getRenameToPartition());

        ReplicationOperation operation =
                job.getPersistedJobInfo().getOperation();
        boolean renameOperation =
                operation == ReplicationOperation.RENAME_TABLE ||
                operation == ReplicationOperation.RENAME_PARTITION;

        if (renameOperation) {
            LOG.info(String.format(
                    "Processing audit log id: %s, job id: %s, " +
                            "operation: %s, source objects: %s " +
                            "rename to: %s",
                    job.getPersistedJobInfo()
                            .getExtras()
                            .get(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY),
                    job.getId(),
                    job.getPersistedJobInfo().getOperation(),
                    srcSpecs,
                    renameToSpec));
        } else {
            LOG.info(String.format(
                            "Processing audit log id: %s, job id: %s, " +
                                    "operation: %s, source objects: %s",
                    job.getPersistedJobInfo()
                            .getExtras()
                            .get(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY),
                    job.getId(),
                    job.getPersistedJobInfo().getOperation(),
                    srcSpecs));
        }
    }
}
