package com.airbnb.di.hive.replication;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveUtils;
import com.airbnb.di.hive.common.NamedPartition;
import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.hooks.HiveOperation;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.filter.ReplicationFilter;
import com.airbnb.di.hive.replication.primitives.CopyPartitionTask;
import com.airbnb.di.hive.replication.primitives.CopyPartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.CopyPartitionsTask;
import com.airbnb.di.hive.replication.primitives.CopyUnpartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.DropPartitionTask;
import com.airbnb.di.hive.replication.primitives.DropTableTask;
import com.airbnb.di.hive.replication.primitives.RenamePartitionTask;
import com.airbnb.di.hive.replication.primitives.RenameTableTask;
import com.airbnb.di.hive.replication.primitives.ReplicationTask;
import com.airbnb.di.multiprocessing.ParallelJobExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ReplicationJobFactory {

    private static final Log LOG = LogFactory.getLog(
            ReplicationJobFactory.class);

    private Configuration conf;
    private Cluster srcCluster;
    private Cluster destCluster;
    private PersistedJobInfoStore jobInfoStore;
    private DestinationObjectFactory destinationObjectFactory;
    private OnStateChangeHandler onStateChangeHandler;
    private ObjectConflictHandler objectConflictHandler;
    private ParallelJobExecutor copyPartitionJobExecutor;
    private DirectoryCopier directoryCopier;

    public ReplicationJobFactory(Configuration conf,
                                 Cluster srcCluster,
                                 Cluster destCluster,
                                 PersistedJobInfoStore jobInfoStore,
                                 DestinationObjectFactory destinationObjectFactory,
                                 OnStateChangeHandler onStateChangeHandler,
                                 ObjectConflictHandler objectConflictHandler,
                                 ParallelJobExecutor copyPartitionJobExecutor,
                                 DirectoryCopier directoryCopier) {
        this.conf = conf;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.jobInfoStore = jobInfoStore;
        this.destinationObjectFactory = destinationObjectFactory;
        this.onStateChangeHandler = onStateChangeHandler;
        this.objectConflictHandler = objectConflictHandler;
        this.copyPartitionJobExecutor = copyPartitionJobExecutor;
        this.directoryCopier = directoryCopier;
    }

    public ReplicationJob createJobForCopyTable(long auditLogId,
                                                long auditLogEntryCreateTime,
                                                Table t)
            throws IOException, SQLException {
        ReplicationOperation replicationOperation =
                HiveUtils.isPartitioned(t) ?
                        ReplicationOperation.COPY_PARTITIONED_TABLE :
                        ReplicationOperation.COPY_UNPARTITIONED_TABLE;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                ReplicationUtils.getLocation(t),
                srcCluster.getName(),
                new HiveObjectSpec(t),
                Collections.emptyList(),
                ReplicationUtils.getTldt(t),
                Optional.empty(),
                Optional.empty(),
                extras);

        HiveObjectSpec spec = new HiveObjectSpec(t);
        Optional<Path> tableLocation = ReplicationUtils.getLocation(t);

        switch (replicationOperation) {
            case COPY_UNPARTITIONED_TABLE:
                return new ReplicationJob(new CopyUnpartitionedTableTask(
                        conf,
                        destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        spec,
                        tableLocation,
                        directoryCopier,
                        true),
                        onStateChangeHandler,
                        persistedJobInfo);
            case COPY_PARTITIONED_TABLE:
                return new ReplicationJob(new CopyPartitionedTableTask(
                        conf,
                        destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        spec,
                        tableLocation
                ), onStateChangeHandler, persistedJobInfo);
            default:
                throw new RuntimeException("Unhandled operation " +
                        replicationOperation);
        }
    }

    public ReplicationJob createJobForCopyPartition(long auditLogId,
                                                    long auditLogEntryCreateTime,
                                                    HiveObjectSpec spec) {
        ReplicationOperation replicationOperation =
                ReplicationOperation.COPY_PARTITION;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(spec.getPartitionName());

        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                Optional.empty(),
                srcCluster.getName(),
                spec,
                partitionNames,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                extras);

        ReplicationTask replicationTask = new CopyPartitionTask(
                conf,
                destinationObjectFactory,
                objectConflictHandler,
                srcCluster,
                destCluster,
                spec,
                Optional.<Path>empty(),
                Optional.<Path>empty(),
                directoryCopier,
                true);

        return new ReplicationJob(replicationTask,
                onStateChangeHandler,
                persistedJobInfo);
    }

    public ReplicationJob createJobForCopyPartition(long auditLogId,
                                                    long auditLogEntryCreateTime,
                                                    NamedPartition namedPartition)
            throws IOException, SQLException {
        String partitionName = namedPartition.getName();
        Partition partition = namedPartition.getPartition();
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(partitionName);

        ReplicationOperation replicationOperation =
                ReplicationOperation.COPY_PARTITION;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        HiveObjectSpec spec = new HiveObjectSpec(namedPartition);
        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                ReplicationUtils.getLocation(partition),
                srcCluster.getName(),
                spec,
                partitionNames,
                ReplicationUtils.getTldt(partition),
                Optional.empty(),
                Optional.empty(),
                extras);

        ReplicationTask replicationTask = new CopyPartitionTask(
                conf,
                destinationObjectFactory,
                objectConflictHandler,
                srcCluster,
                destCluster,
                spec,
                ReplicationUtils.getLocation(partition),
                Optional.<Path>empty(),
                directoryCopier,
                true);

        return new ReplicationJob(replicationTask,
                onStateChangeHandler,
                persistedJobInfo);
    }

    public ReplicationJob createJobForCopyDynamicPartitions(
            long auditLogId,
            long auditLogEntryCreateTime,
            List<NamedPartition> namedPartition)
            throws IOException, SQLException {

        ReplicationOperation replicationOperation =
                ReplicationOperation.COPY_PARTITIONS;

        List<Partition> partitions = NamedPartition.toPartitions(
                namedPartition);
        List<String> partitionNames = NamedPartition.toNames(
                namedPartition);

        // The common location is the common path that all the partitions share.
        Optional<Path> commonLocation = ReplicationUtils.getCommonDirectory(
                ReplicationUtils.getLocations(partitions));

        Partition samplePartition = namedPartition.get(0).getPartition();
        HiveObjectSpec tableSpec = new HiveObjectSpec(
                samplePartition.getDbName(),
                samplePartition.getTableName());

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                commonLocation,
                srcCluster.getName(),
                tableSpec,
                partitionNames,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                extras);

        ReplicationTask replicationTask = new CopyPartitionsTask(
                conf,
                destinationObjectFactory,
                objectConflictHandler,
                srcCluster,
                destCluster,
                tableSpec,
                partitionNames,
                commonLocation,
                copyPartitionJobExecutor,
                directoryCopier);

        return new ReplicationJob(replicationTask, onStateChangeHandler,
                persistedJobInfo);
    }

    private Map<HiveObjectSpec, Table> createTableLookupMap(
            List<Table> tables) {
        // Create a map from the table spec to the table object. We'll need this
        // for getting the table that a partition belongs to
        Map<HiveObjectSpec, Table> specToTable = new HashMap<>();
        for (Table table : tables) {
            HiveObjectSpec spec = new HiveObjectSpec(table);
            specToTable.put(spec, table);
        }
        return specToTable;
    }

    public ReplicationJob createJobForDropTable(long auditLogId,
                                                long auditLogEntryCreateTime,
                                                Table table)
            throws IOException, SQLException {
        ReplicationOperation replicationOperation =
                ReplicationOperation.DROP_TABLE;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        HiveObjectSpec tableSpec =
                new HiveObjectSpec(table);

        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                ReplicationUtils.getLocation(table),
                srcCluster.getName(),
                tableSpec,
                Collections.emptyList(),
                ReplicationUtils.getTldt(table),
                Optional.empty(),
                Optional.empty(),
                extras);

        return new ReplicationJob(
                new DropTableTask(
                        srcCluster,
                        destCluster,
                        tableSpec,
                        ReplicationUtils.getTldt(table)),
                onStateChangeHandler,
                persistedJobInfo);
    }

    public ReplicationJob createJobForDropPartition(long auditLogId,
                                                    long auditLogEntryCreateTime,
                                                    NamedPartition namedPartition)
            throws IOException, SQLException {
        ReplicationOperation replicationOperation =
                ReplicationOperation.DROP_PARTITION;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        HiveObjectSpec partitionSpec =
                new HiveObjectSpec(namedPartition);
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(namedPartition.getName());
        Optional<String> partitionTldt =
                ReplicationUtils.getTldt(namedPartition.getPartition());
        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                ReplicationUtils.getLocation(namedPartition.getPartition()),
                srcCluster.getName(),
                partitionSpec.getTableSpec(),
                partitionNames,
                partitionTldt,
                Optional.empty(),
                Optional.empty(),
                extras);

        return new ReplicationJob(
                new DropPartitionTask(
                        srcCluster,
                        destCluster,
                        partitionSpec,
                        partitionTldt),
                onStateChangeHandler,
                persistedJobInfo);
    }

    public ReplicationJob createJobForRenameTable(long auditLogId,
                                                  long auditLogEntryCreateTime,
                                                  Table renameFromTable,
                                                  Table renameToTable)
            throws IOException, SQLException {
        ReplicationOperation replicationOperation =
                ReplicationOperation.RENAME_TABLE;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        HiveObjectSpec renameFromTableSpec =
                new HiveObjectSpec(renameFromTable);
        HiveObjectSpec renameToTableSpec =
                new HiveObjectSpec(renameToTable);
        Optional<Path> renameFromPath =
                ReplicationUtils.getLocation(renameFromTable);
        Optional<Path> renameToPath =
                ReplicationUtils.getLocation(renameToTable);

        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                renameFromPath,
                srcCluster.getName(),
                renameFromTableSpec,
                new ArrayList<>(),
                ReplicationUtils.getTldt(renameFromTable),
                Optional.of(renameToTableSpec),
                renameToPath,
                extras);

        return new ReplicationJob(
                new RenameTableTask(
                        conf,
                        srcCluster,
                        destCluster,
                        destinationObjectFactory,
                        objectConflictHandler,
                        renameFromTableSpec,
                        renameToTableSpec,
                        renameFromPath,
                        renameToPath,
                        ReplicationUtils.getTldt(renameFromTable),
                        copyPartitionJobExecutor,
                        directoryCopier),
                onStateChangeHandler,
                persistedJobInfo);
    }

    public ReplicationJob createJobForRenamePartition(
            long auditLogId,
            long auditLogEntryCreateTime,
            NamedPartition renameFromPartition,
            NamedPartition renameToPartition)
            throws IOException, SQLException {
        ReplicationOperation replicationOperation =
                ReplicationOperation.RENAME_PARTITION;

        Map<String, String> extras = new HashMap<>();
        extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY,
                Long.toString(auditLogId));
        extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
                Long.toString(auditLogEntryCreateTime));

        HiveObjectSpec renameFromPartitionSpec =
                new HiveObjectSpec(renameFromPartition);
        HiveObjectSpec renameToPartitionSpec =
                new HiveObjectSpec(renameToPartition);
        Optional renameFromPath = ReplicationUtils.getLocation(
                renameFromPartition.getPartition());
        Optional renameToPath = ReplicationUtils.getLocation(
                renameToPartition.getPartition());

        PersistedJobInfo persistedJobInfo = jobInfoStore.resilientCreate(
                replicationOperation,
                ReplicationStatus.PENDING,
                renameFromPath,
                srcCluster.getName(),
                renameFromPartitionSpec,
                new ArrayList<>(),
                ReplicationUtils.getTldt(renameFromPartition.getPartition()),
                Optional.of(renameToPartitionSpec),
                renameToPath,
                extras);

        return new ReplicationJob(
                new RenamePartitionTask(
                        conf,
                        destinationObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        renameFromPartitionSpec,
                        renameToPartitionSpec,
                        renameFromPath,
                        renameToPath,
                        ReplicationUtils.getTldt(
                                renameFromPartition.getPartition()),
                        directoryCopier),
                onStateChangeHandler,
                persistedJobInfo);
    }

    private enum OperationType {COPY, DROP, RENAME}

    /**
     * Converts the audit log entry into a set of replication jobs that have
     * the persisted elements properly set.
     * @param auditLogEntry
     * @throws IOException
     * @throws SQLException
     */
    public List<ReplicationJob> createReplicationJobs(
            AuditLogEntry auditLogEntry,
            ReplicationFilter replicationFilter)
            throws IOException, SQLException {
        List<ReplicationJob> replicationJobs = new ArrayList<>();

        if (!replicationFilter.accept(auditLogEntry)) {
            LOG.debug(String.format("Audit log entry id: %s filtered out by %s",
                    auditLogEntry,
                    replicationFilter.getClass().getSimpleName()));
            return replicationJobs;
        }


        // TODO: Rewrite once HIVE-12215 is resolved.
        // The inputs and outputs for exchange partitions in the audit log is
        // broken due to HIVE-12215. This workaround is to parse the exchange
        // partition command to figure out what the input and output partitions
        // are.
        if (auditLogEntry.getOutputTables().size() == 0 &&
                auditLogEntry.getCommandType() == null) {
            // This is probably an exchange partition command
            ExchangePartitionParser parser = new ExchangePartitionParser();
            boolean parsed = parser.parse(auditLogEntry.getCommand());
            if (parsed) {
                LOG.debug(String.format("Parsed audit log id: %s " +
                                "query: %s as an exchange partition query",
                        auditLogEntry.getId(),
                        auditLogEntry.getCommand()));
                // Since we're missing the modified time for the source
                // partition, just copy for now

                HiveObjectSpec exchangeToSpec = new HiveObjectSpec(
                        parser.getExchangeToSpec().getDbName(),
                        parser.getExchangeToSpec().getTableName(),
                        parser.getPartitionName());

                Table exchangeToTable = new Table();
                exchangeToTable.setDbName(exchangeToSpec.getDbName());
                exchangeToTable.setTableName(exchangeToSpec.getTableName());

                Partition exchangeToPartition = new Partition();
                exchangeToPartition.setDbName(exchangeToSpec.getDbName());
                exchangeToPartition.setTableName(exchangeToSpec.getTableName());
                exchangeToPartition.setValues(parser.getPartitionValues());
                if (!replicationFilter.accept(
                        exchangeToTable,
                        new NamedPartition(exchangeToSpec.getPartitionName(),
                                exchangeToPartition))) {
                    LOG.debug(String.format("Exchange partition from audit log" +
                                    " id: %s filtered out by %s",
                            auditLogEntry.getId(),
                            replicationFilter.getClass().getSimpleName()));
                    return replicationJobs;
                } else {
                    ReplicationJob job = createJobForCopyPartition(
                            auditLogEntry.getId(),
                            auditLogEntry.getCreateTime().getTime(),
                            exchangeToSpec
                    );

                    replicationJobs.add(job);
                    return replicationJobs;
                }
            } else {
                LOG.warn("Error parsing query " + auditLogEntry.getCommand());
            }
        }
        // End exchange partitions workaround

        if (auditLogEntry.getOutputTables().size() == 0 &&
                auditLogEntry.getOutputPartitions().size() == 0) {
            LOG.debug(String.format("Audit log entry id: %s filtered out " +
                            "since it has no output tables or partitions",
                    auditLogEntry.getId()));
            return replicationJobs;
        }

        OperationType operationType = null;
        switch(auditLogEntry.getCommandType()) {
            case DROPTABLE:
            case DROPVIEW:
            case ALTERTABLE_DROPPARTS:
                operationType = OperationType.DROP;
                break;
            case ALTERTABLE_RENAME:
            case ALTERVIEW_RENAME:
            case ALTERTABLE_RENAMEPART:
                operationType = OperationType.RENAME;
                break;
            default:
                operationType = OperationType.COPY;
        }

        List<Table> outputTables = new ArrayList<>(
                auditLogEntry.getOutputTables());
        List<NamedPartition> outputPartitions =
                new ArrayList<>(auditLogEntry.getOutputPartitions());
        List<Table> referenceTables = auditLogEntry.getReferenceTables();

        // Filter out tables and partitions that we may not want to replicate
        filterObjects(replicationFilter,
                outputTables,
                outputPartitions,
                createTableLookupMap(referenceTables));

        switch(operationType) {
            case COPY:
                // Handle the tables. The table is present in add partition
                // calls, so skip in those cases.
                if (auditLogEntry.getCommandType() !=
                        HiveOperation.ALTERTABLE_ADDPARTS) {
                    for (Table t : outputTables) {
                        replicationJobs.add(
                                createJobForCopyTable(auditLogEntry.getId(),
                                        auditLogEntry.getCreateTime().getTime(),
                                        t));
                    }
                }

                // Handle the partitions
                // See if this is a dynamic partition insert
                if (auditLogEntry.getOutputPartitions().size() > 1 &&
                        ReplicationUtils.fromSameTable(
                                NamedPartition.toPartitions(outputPartitions))) {
                    replicationJobs.add(
                            createJobForCopyDynamicPartitions(
                                    auditLogEntry.getId(),
                                    auditLogEntry.getCreateTime().getTime(),
                                    auditLogEntry.getOutputPartitions()));
                } else {
                    // Otherwise create separate insert partition jobs for each
                    // partition
                    for (NamedPartition p : outputPartitions) {
                        replicationJobs.add(
                                createJobForCopyPartition(
                                        auditLogEntry.getId(),
                                        auditLogEntry.getCreateTime().getTime(),
                                        p));
                    }
                }
                break;
            case DROP:
                for (Table t : outputTables) {
                    replicationJobs.add(
                            createJobForDropTable(
                                    auditLogEntry.getId(),
                                    auditLogEntry.getCreateTime().getTime(),
                                    t));
                }
                for (NamedPartition p : outputPartitions) {
                    replicationJobs.add(
                            createJobForDropPartition(
                                    auditLogEntry.getId(),
                                    auditLogEntry.getCreateTime().getTime(),
                                    p));
                }
                break;
            case RENAME:
                // There's an edge case to consider - let's say table A is
                // renamed to table B, however, table A is excluded by the
                // user specified filter. In this case, we still do the rename.
                if (outputTables.size() == 0 && outputPartitions.size() == 0) {
                    // This means that the table was filtered out
                } else  if (auditLogEntry.getRenameFromTable() != null) {
                    // Handle a rename table
                    replicationJobs.add(
                            createJobForRenameTable(
                                    auditLogEntry.getId(),
                                    auditLogEntry.getCreateTime().getTime(),
                                    auditLogEntry.getRenameFromTable(),
                                    auditLogEntry.getOutputTables().get(0)));
                } else if (auditLogEntry.getRenameFromPartition() != null) {
                    // Handle a rename partition
                    replicationJobs.add(
                            createJobForRenamePartition(
                                    auditLogEntry.getId(),
                                    auditLogEntry.getCreateTime().getTime(),
                                    auditLogEntry.getRenameFromPartition(),
                                    auditLogEntry.getOutputPartitions().get(0)));
                } else {
                    throw new RuntimeException("Shouldn't happen!");
                }
                break;
            default:
                throw new RuntimeException("Operation not handled: " +
                        operationType);
        }

        LOG.debug("Converted audit log entry " + auditLogEntry + " to " +
                replicationJobs);

        return replicationJobs;
    }

    /**
     * Based on the supplied filter, remove tables and partitions that should
     * not be replicated.
     * @param filter
     * @param tables
     * @param partitions
     */
    private void filterObjects(ReplicationFilter filter,
                               List<Table> tables,
                               List<NamedPartition> partitions,
                               Map<HiveObjectSpec, Table> tableLookupMap) {

        // Create the list of tables that the partitions belong to. These
        // tables were included by the hook, but don't need to be replicated,
        // but is needed for running the filter.
        Set<HiveObjectSpec> tablesToNotReplicate = new HashSet<>();
        for (NamedPartition pwn : partitions) {
            Partition p = pwn.getPartition();
            HiveObjectSpec tableSpec = new HiveObjectSpec(p.getDbName(),
                    p.getTableName());
            tablesToNotReplicate.add(tableSpec);
        }

        // Remove all the partitions that don't match the filter
        Iterator<NamedPartition> partitionIterator = partitions.iterator();
        while(partitionIterator.hasNext()) {
            NamedPartition pwn = partitionIterator.next();
            Partition partition = pwn.getPartition();
            HiveObjectSpec partitionSpec = new HiveObjectSpec(pwn);

            Table table = tableLookupMap.get(new HiveObjectSpec(
                    partition.getDbName(), partition.getTableName()));
            if (!filter.accept(table, pwn)) {
                LOG.debug(String.format("%s filtering out: %s",
                        filter.getClass().getName(),
                        partitionSpec));
                partitionIterator.remove();
            }
        }

        // Remove all tables that don't pass the filter, or don't need to be
        // replicated
        Iterator<Table> tableIterator = tables.iterator();
        while(tableIterator.hasNext()) {
            Table table = tableIterator.next();
            HiveObjectSpec tableSpec = new HiveObjectSpec(table);
            if (!filter.accept(table)) {
                LOG.debug(String.format("%s filtering out: %s",
                        filter.getClass().getName(),
                        tableSpec));
                tableIterator.remove();
            }

        }
    }
}
