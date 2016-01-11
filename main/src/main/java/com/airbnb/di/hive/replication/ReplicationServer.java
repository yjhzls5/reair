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
import com.airbnb.di.hive.replication.primitives.RenamePartitionTask;
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
import java.util.Optional;
import java.util.TimeZone;

public class ReplicationServer implements TReplicationService.Iface {

    private static final Log LOG = LogFactory.getLog(
            ReplicationServer.class);

    private final long POLL_WAIT_TIME_MS = 10 * 1000;

    // If there is a need to wait to poll, wait this many ms
    private long pollWaitTimeMs = POLL_WAIT_TIME_MS;

    // Key used for storing the last persisted audit log ID in the key value
    // store
    public static final String LAST_PERSISTED_AUDIT_LOG_ID_KEY =
            "last_persisted_id";

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

    private Optional<Long> startAfterAuditLogId = Optional.empty();

    // Responsible for persisting changes to the state of the replication job
    // once it finishes
    private class JobStateChangeHandler implements OnStateChangeHandler {
        @Override
        public void onStart(ReplicationJob replicationJob) {
            LOG.debug("Job id: " + replicationJob.getId() + " started");
            jobInfoStore.changeStautsAndPersist(
                    ReplicationStatus.RUNNING,
                    replicationJob.getPersistedJobInfo());
        }

        @Override
        public void onComplete(RunInfo runInfo,
                               ReplicationJob replicationJob) {
            LOG.debug("Job id: " + replicationJob.getId() + " finished " +
                    "with state " + runInfo.getRunStatus() + " and " +
                    runInfo.getBytesCopied() + " bytes copied");

            replicationJob.getPersistedJobInfo().getExtras().put(
                    PersistedJobInfo.BYTES_COPIED_KEY,
                    Long.toString(runInfo.getBytesCopied()));


            LOG.debug("Persisting job id: " +
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
            LOG.debug("Persisted job: " + replicationJob);
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
                             Optional<Long> startAfterAuditLogId) {
        this.conf = conf;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.auditLogReader = auditLogReader;
        this.keyValueStore = keyValueStore;
        this.jobInfoStore = jobInfoStore;

        this.onStateChangeHandler = new JobStateChangeHandler();

        this.objectConflictHandler = new ObjectConflictHandler();
        this.objectConflictHandler.setConf(conf);
        this.destinationObjectFactory = new DestinationObjectFactory();
        this.destinationObjectFactory.setConf(conf);

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

        if (persistedJobInfo.getSrcPartitionNames().size() > 0) {
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
                        directoryCopier,
                        true);
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
                        Optional.empty(),
                        directoryCopier,
                        true);
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
                if (!persistedJobInfo.getRenameToDb().isPresent() ||
                    !persistedJobInfo.getRenameToTable().isPresent()) {
                    throw new RuntimeException(String.format(
                            "Rename to table is invalid: %s.%s",
                            persistedJobInfo.getRenameToDb(),
                            persistedJobInfo.getRenameToTable()));
                }
                HiveObjectSpec renameToTableSpec = new HiveObjectSpec(
                        persistedJobInfo.getRenameToDb().get(),
                        persistedJobInfo.getRenameToTable().get());

                replicationTask = new RenameTableTask(
                        conf,
                        srcCluster,
                        destCluster,
                        destinationObjectFactory,
                        objectConflictHandler,
                        tableSpec,
                        renameToTableSpec,
                        persistedJobInfo.getSrcPath(),
                        persistedJobInfo.getRenameToPath(),
                        persistedJobInfo.getSrcObjectTldt(),
                        copyPartitionJobExecutor,
                        directoryCopier);
                break;
            case RENAME_PARTITION:
                // TODO: Handle rename partition
            default:
                throw new UnsupportedOperationException("Unhandled operation:" +
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

        if (startAfterAuditLogId.isPresent()) {
            // The starting ID was specified
            lastPersistedAuditLogId = startAfterAuditLogId.get();
        } else {
            // Otherwise, start from the previous stop point
            LOG.debug("Fetching last persisted audit log ID");
            Optional<String> lastPersistedIdString = keyValueStore.get(
                    LAST_PERSISTED_AUDIT_LOG_ID_KEY);

            if (!lastPersistedIdString.isPresent()) {
                Optional<Long> maxId = auditLogReader.getMaxId();
                lastPersistedAuditLogId = maxId.orElse(Long.valueOf(0));
                LOG.warn(String.format("Since the last persisted ID was not " +
                        "previously set, using max ID in the audit log: %s",
                        lastPersistedAuditLogId));

            } else {
                lastPersistedAuditLogId = Long.parseLong(
                        lastPersistedIdString.get());
            }
        }

        LOG.info("Using last persisted ID of " + lastPersistedAuditLogId);
        auditLogReader.setReadAfterId(lastPersistedAuditLogId);

        // Resume jobs that were persisted, but were not run.
        for(PersistedJobInfo jobInfo : jobInfoStore.getRunnableFromDb()) {
            LOG.debug(String.format("Restoring %s to (re)run", jobInfo));
            ReplicationJob job = restoreReplicationJob(jobInfo);
            prettyLogStart(job);
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
                LOG.debug("Pause requested. Sleeping...");
                ReplicationUtils.sleep(pollWaitTimeMs);
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
                LOG.debug(String.format("Hit the limit for the number of " +
                "successful jobs (%d) - returning.", jobsToComplete));
                return;
            }

            //  Wait if there are too many jobs
            if (jobExecutor.getNotDoneJobCount() > maxJobsInMemory) {
                LOG.debug(String.format("There are too many jobs in memory. " +
                        "Waiting until more complete. (limit: %d)",
                        maxJobsInMemory));
                ReplicationUtils.sleep(pollWaitTimeMs);
                continue;
            }

            // Get an entry from the audit log
            LOG.debug("Fetching the next entry from the audit log");
            Optional<AuditLogEntry> auditLogEntry =
                    auditLogReader.resilientNext();

            // If there's nothing from the audit log, then wait for a little bit
            // and then try again.
            if (!auditLogEntry.isPresent()) {
                LOG.debug(String.format("No more entries from the audit log. " +
                        "Sleeping for %s ms", pollWaitTimeMs));
                ReplicationUtils.sleep(pollWaitTimeMs);
                continue;
            }

            AuditLogEntry entry = auditLogEntry.get();

            LOG.debug("Got audit log entry: " + entry);

            // Convert the audit log entry into a replication job, which has
            // elements persisted to the DB
            List<ReplicationJob> replicationJobs =
                    jobFactory.createReplicationJobs(auditLogEntry.get(),
                            replicationFilter);

            LOG.debug(String.format("Audit log entry id: %s converted to %s",
                    entry.getId(),
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
                        Long.toString(entry.getId()));
                updateTimeForLastPersistedId = System.currentTimeMillis();
            }

            for (ReplicationJob replicationJob : replicationJobs) {
                LOG.debug("Scheduling: " + replicationJob);
                prettyLogStart(replicationJob);
                long tasksSubmittedForExecution = counters.getCounter(
                        ReplicationCounters.Type.EXECUTION_SUBMITTED_TASKS);

                if (tasksSubmittedForExecution >= jobsToComplete) {
                    LOG.warn(String.format("Not submitting %s for execution " +
                                    " due to the limit for the number of " +
                                    "jobs to execute", replicationJob));
                    continue;
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
        List<TReplicationJob> jobsToReturn = new ArrayList<>();

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
        return jobsToReturn;
    }

    @Override
    synchronized public void pause() throws TException {
        LOG.debug("Paused requested");
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
        LOG.debug("Resume requested");
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
        List<TReplicationJob> jobsToReturn = new ArrayList<>();

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
        return jobsToReturn;
    }

    private void prettyLogStart(ReplicationJob job) {
        List<HiveObjectSpec> srcSpecs = new ArrayList<>();

        if (job.getPersistedJobInfo().getSrcPartitionNames().size() > 0) {
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

        Optional<HiveObjectSpec> renameToSpec = Optional.empty();
        PersistedJobInfo jobInfo = job.getPersistedJobInfo();
        if (jobInfo.getRenameToDb().isPresent() &&
                jobInfo.getRenameToTable().isPresent()) {
            if (!jobInfo.getRenameToPartition().isPresent()) {
                renameToSpec = Optional.of(
                        new HiveObjectSpec(jobInfo.getRenameToDb().get(),
                                jobInfo.getRenameToTable().get()));
            } else {
                renameToSpec = Optional.of(
                        new HiveObjectSpec(
                                jobInfo.getRenameToDb().get(),
                                jobInfo.getRenameToTable().get(),
                                jobInfo.getRenameToPartition().get()));
            }
        }
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

    /**
     * Start processing audit log entries after this ID. This should only be
     * called before run() is called.
     */
    public void setStartAfterAuditLogId(long auditLogId) {
        this.startAfterAuditLogId = Optional.of(auditLogId);
    }

    /**
     * For polling operations that need to sleep, sleep for this many
     * milliseconds.
     */
    public void setPollWaitTimeMs(long pollWaitTimeMs) {
        this.pollWaitTimeMs = pollWaitTimeMs;
    }
}
