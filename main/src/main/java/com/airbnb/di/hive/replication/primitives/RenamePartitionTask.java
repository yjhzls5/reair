package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveUtils;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.multiprocessing.Lock;
import com.airbnb.di.multiprocessing.LockSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class RenamePartitionTask implements ReplicationTask {

    private static final Log LOG = LogFactory.getLog(
            RenamePartitionTask.class);

    private Configuration conf;
    private DestinationObjectFactory destObjectFactory;
    private ObjectConflictHandler objectConflictHandler;
    private Cluster srcCluster;
    private Cluster destCluster;
    private HiveObjectSpec renameFromSpec;
    private HiveObjectSpec renameToSpec;
    private Path renameFromPath;
    private Path renameToPath;
    private String renameFromPartitionTdlt;
    private DirectoryCopier directoryCopier;

    public RenamePartitionTask(Configuration conf,
                               DestinationObjectFactory destObjectFactory,
                               ObjectConflictHandler objectConflictHandler,
                               Cluster srcCluster,
                               Cluster destCluster,
                               HiveObjectSpec renameFromSpec,
                               HiveObjectSpec renameToSpec,
                               Path renameFromPath,
                               Path renameToPath,
                               String renameFromPartitionTdlt,
                               DirectoryCopier directoryCopier) {
        this.conf = conf;
        this.destObjectFactory = destObjectFactory;
        this.objectConflictHandler = objectConflictHandler;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.renameFromSpec = renameFromSpec;
        this.renameToSpec = renameToSpec;
        this.renameFromPath = renameFromPath;
        this.renameToPath = renameToPath;
        this.renameFromPartitionTdlt = renameFromPartitionTdlt;
        this.directoryCopier = directoryCopier;
    }

    enum HandleRenameAction {
        RENAME_PARTITION,
        EXCHANGE_PARTITION,
        COPY_PARTITION,
        NO_OP
    };

    @Override
    public RunInfo runTask() throws HiveMetastoreException, DistCpException, IOException {
        LOG.info("Renaming " + renameFromSpec + " to " + renameToSpec);

        HiveMetastoreClient destMs = destCluster.getMetastoreClient();
        HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

        Partition freshSrcRenameToPart = srcMs.getPartition(
                renameToSpec.getDbName(),
                renameToSpec.getTableName(),
                renameToSpec.getPartitionName());

        Partition freshDestRenameToPart = destMs.getPartition(
                renameToSpec.getDbName(),
                renameToSpec.getTableName(),
                renameToSpec.getPartitionName());

        // Get a fresh copy of the metadata from the source Hive metastore
        Partition freshDestRenameFromPart = destMs.getPartition(
                renameFromSpec.getDbName(),
                renameFromSpec.getTableName(),
                renameFromSpec.getPartitionName());


        HandleRenameAction renameAction = null;
        // For cases where the table doesn't change, you can do a rename
        if (renameFromSpec.getDbName().equals(
                renameToSpec.getDbName()) &&
                renameFromSpec.getTableName().equals(
                        renameToSpec.getTableName())) {
            renameAction = HandleRenameAction.RENAME_PARTITION;
        } else {
            // Otherwise, it needs to be an exchange.
            renameAction = HandleRenameAction.EXCHANGE_PARTITION;
        }

        // Check to see if transient_lastDdl times match between what was
        // renamed and what exists
        if (ReplicationUtils.transientLastDdlTimesMatch(
                freshSrcRenameToPart,
                freshDestRenameToPart)) {
            LOG.info("Rename to partition exists on destination and has a " +
                    "matching TLDT. Not doing anything");
            renameAction = HandleRenameAction.NO_OP;

        } else if (freshDestRenameToPart != null) {
            LOG.info("Rename to partition already exists on destination, but " +
                    "doesn't have a matching TLDT. Copying instead...");
            renameAction = HandleRenameAction.COPY_PARTITION;
        } else if (freshDestRenameFromPart == null) {
            LOG.warn(StringUtils.format("Renamed-from partition %s " +
                            "doesn't exist. " +
                            "Copying %s to destination instead.",
                    renameFromSpec,
                    renameToSpec));
            renameAction = HandleRenameAction.COPY_PARTITION;
        } else if (!ReplicationUtils.transientLastDdlTimesMatch(
                renameFromPartitionTdlt,
                freshDestRenameFromPart)) {
            LOG.warn(StringUtils.format("Destination partition %s doesn't " +
                    "have the expected modified time", renameFromSpec));
            LOG.info("Renamed from source table with a TLDT: " +
                    renameFromPartitionTdlt);
            LOG.info("Partition on destination: " + freshDestRenameFromPart);
            LOG.info(String.format("Copying %s to destination instead",
                    renameToSpec));
            renameAction = HandleRenameAction.COPY_PARTITION;
        } else {
            LOG.info(String.format("Destination table (%s) matches " +
                            "expected TLDT(%s) - will rename", renameFromSpec,
                    ReplicationUtils.getTldt(freshDestRenameFromPart)));
            // Action set in the beginning
        }

        switch(renameAction) {
            case NO_OP:
                return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);

            case RENAME_PARTITION:
                LOG.info(StringUtils.format("Renaming %s to %s",
                        renameFromSpec, renameToSpec));
                Partition newPartitionOnDestination = new Partition(
                        freshDestRenameFromPart);

                List<String> renameToPartitionValues =
                        HiveUtils.partitionNameToValues(
                                destMs,
                                renameToSpec.getPartitionName());

                List<String> renameFromPartitionValues =
                        HiveUtils.partitionNameToValues(
                                srcMs,
                                renameFromSpec.getPartitionName());

                newPartitionOnDestination.setValues(
                        renameToPartitionValues);

                destMs.renamePartition(renameFromSpec.getDbName(),
                        renameFromSpec.getTableName(),
                        renameFromPartitionValues,
                        newPartitionOnDestination);
                LOG.info(StringUtils.format("Renamed %s to %s", renameFromSpec,
                        renameToSpec));

                // After a rename, the partition should be re-copied to get the
                // correct modified time changes. With a proper rename, this
                // should be a mostly no-op.
                return copyPartition(renameToSpec, renameToPath);

            case EXCHANGE_PARTITION:
                // TODO: Exchange partition can't be done without HIVE-12215
                // Just do a copy instead.

            case COPY_PARTITION:
                return copyPartition(renameToSpec, renameToPath);

            default:
                throw new RuntimeException("Unhandled case: " + renameAction);
        }
    }

    private RunInfo copyPartition(HiveObjectSpec spec, Path partitionLocation)
            throws HiveMetastoreException, DistCpException, IOException {
        CopyPartitionTask task = new CopyPartitionTask(
                conf,
                destObjectFactory,
                objectConflictHandler,
                srcCluster,
                destCluster,
                spec,
                partitionLocation,
                null,
                directoryCopier,
                true);
        return task.runTask();
    }

    @Override
    public LockSet getRequiredLocks() {
        LockSet lockSet = new LockSet();
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameFromSpec.toString()));
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameToSpec.toString()));
        return lockSet;
    }


}
