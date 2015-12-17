package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.multiprocessing.Lock;
import com.airbnb.di.multiprocessing.LockSet;
import com.airbnb.di.multiprocessing.ParallelJobExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class RenameTableTask implements ReplicationTask {
    private static final Log LOG = LogFactory.getLog(
            RenameTableTask.class);

    private Configuration conf;
    private DestinationObjectFactory destObjectFactory;
    private ObjectConflictHandler objectConflictHandler;
    private Cluster srcCluster;
    private Cluster destCluster;
    private HiveObjectSpec renameFromSpec;
    private HiveObjectSpec renameToSpec;
    private Path renameFromPath;
    private Path renameToPath;
    private String renameFromTableTdlt;

    private ParallelJobExecutor copyPartitionsExecutor;
    private DirectoryCopier directoryCopier;

    public RenameTableTask(Configuration conf,
                           Cluster srcCluster,
                           Cluster destCluster,
                           DestinationObjectFactory destObjectFactory,
                           ObjectConflictHandler objectConflictHandler,
                           HiveObjectSpec renameFromSpec,
                           HiveObjectSpec renameToSpec,
                           Path renameFromPath,
                           Path renameToPath,
                           String renameFromTableTldt,
                           ParallelJobExecutor copyPartitionsExecutor,
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
        this.renameFromTableTdlt = renameFromTableTldt;

        this.copyPartitionsExecutor = copyPartitionsExecutor;
        this.directoryCopier = directoryCopier;
    }

    enum HandleRenameAction {RENAME_TABLE, COPY_TABLE, NO_OP};

    @Override
    public RunInfo runTask() throws DistCpException, HiveMetastoreException,
            IOException {
        LOG.info("Renaming " + renameFromSpec + " to " + renameToSpec);

        HiveMetastoreClient destMs = destCluster.getMetastoreClient();
        HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

        Table freshSrcRenameToTable = srcMs.getTable(renameToSpec.getDbName(),
                renameToSpec.getTableName());

        Table freshDestRenameToTable = destMs.getTable(renameToSpec.getDbName(),
                renameToSpec.getTableName());

        // Get a fresh copy of the metadata from the dest Hive metastore
        Table freshDestTable = destMs.getTable(renameFromSpec.getDbName(),
                renameFromSpec.getTableName());

        HandleRenameAction renameAction = null;
        if (ReplicationUtils.transientLastDdlTimesMatch(
                freshSrcRenameToTable,
                freshDestRenameToTable)) {
            LOG.info("Rename to table exists on destination and has a " +
                    "matching TLDT. Not doing anything");
            renameAction = HandleRenameAction.NO_OP;

        } else if (freshDestRenameToTable != null) {
            LOG.info("Rename to table already exists on destination, but " +
                    "doesn't have a matching TLDT. Copying instead...");
            renameAction = HandleRenameAction.COPY_TABLE;
        } else if (freshDestTable == null) {
            LOG.warn(StringUtils.format("Destination rename from table %s " +
                            "doesn't exist. " +
                            "Copying %s to destination instead.",
                    renameFromSpec,
                    renameToSpec));
            renameAction = HandleRenameAction.COPY_TABLE;
        } else if (!ReplicationUtils.transientLastDdlTimesMatch(
                renameFromTableTdlt,
                freshDestTable)) {
            LOG.warn(StringUtils.format("Destination table %s doesn't have " +
                    "the expected modified time", renameFromSpec));
            LOG.info("Renamed from source table with a TLDT: " +
                    renameFromTableTdlt);
            LOG.info("Table on destination: " + freshDestTable);
            LOG.info(String.format("Copying %s to destination instead",
                    renameToSpec));
            renameAction = HandleRenameAction.COPY_TABLE;
        } else {
            LOG.info(String.format("Destination table (%s) matches " +
                    "expected TLDT(%s) - will rename", renameFromSpec,
                    ReplicationUtils.getTldt(freshDestTable)));
            renameAction = HandleRenameAction.RENAME_TABLE;
        }

        switch(renameAction) {
            case NO_OP:
                return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);

            case RENAME_TABLE:
                LOG.info(StringUtils.format("Renaming %s to %s",
                        renameFromSpec, renameToSpec));
                Table newTableOnDestination = new Table(freshDestTable);
                newTableOnDestination.setDbName(renameToSpec.getDbName());
                newTableOnDestination.setTableName(renameToSpec.getTableName());
                destMs.alterTable(renameFromSpec.getDbName(),
                        renameFromSpec.getTableName(), newTableOnDestination);
                LOG.info(StringUtils.format("Renamed %s to %s", renameFromSpec,
                        renameToSpec));
                // After a rename, the table should be re-copied to get the
                // correct modified time changes. With a proper rename, this
                // should be a mostly no-op. Fall through to the next case.

            case COPY_TABLE:
                CopyCompleteTableTask task = new CopyCompleteTableTask(
                        conf,
                        destObjectFactory,
                        objectConflictHandler,
                        srcCluster,
                        destCluster,
                        renameToSpec,
                        renameToPath,
                        copyPartitionsExecutor,
                        directoryCopier
                        );
                return task.runTask();

            default:
                throw new RuntimeException("Unhandled case: " + renameAction);
        }
    }

    @Override
    public LockSet getRequiredLocks() {
        LockSet lockSet = new LockSet();
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameFromSpec.toString()));
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameToSpec.toString()));
        return lockSet;
    }
}
