package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.PathBuilder;
import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveUtils;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.multiprocessing.Lock;
import com.airbnb.di.multiprocessing.LockSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by paul_yang on 6/9/15.
 */
public class CopyUnpartitionedTableTask implements ReplicationTask {
    private static final Log LOG = LogFactory.getLog(CopyUnpartitionedTableTask.class);

    private Configuration conf;
    private DestinationObjectFactory objectModifier;
    private ObjectConflictHandler objectConflictHandler;
    private Cluster srcCluster;
    private Cluster destCluster;
    private HiveObjectSpec spec;
    private Path tableLocation;
    private DirectoryCopier directoryCopier;
    private boolean allowDataCopy;

    public CopyUnpartitionedTableTask(Configuration conf,
                                      DestinationObjectFactory destObjectFactory,
                                      ObjectConflictHandler objectConflictHandler,
                                      Cluster srcCluster,
                                      Cluster destCluster,
                                      HiveObjectSpec spec,
                                      Path tableLocation,
                                      DirectoryCopier directoryCopier,
                                      boolean allowDataCopy) {
        this.conf = conf;
        this.objectModifier = destObjectFactory;
        this.objectConflictHandler = objectConflictHandler;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.tableLocation = tableLocation;
        this.spec = spec;
        this.directoryCopier = directoryCopier;
        this.allowDataCopy = allowDataCopy;
    }

    public RunInfo runTask() throws HiveMetastoreException, DistCpException,
            IOException {
        LOG.info("Copying " + spec);

        HiveMetastoreClient destMs = destCluster.getMetastoreClient();
        HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

        // Get a fresh copy of the metadata from the source Hive metastore
        Table freshSrcTable = srcMs.getTable(spec.getDbName(),
                spec.getTableName());

        if (freshSrcTable == null) {
            LOG.warn("Source table " + spec + " doesn't exist, so not " +
                    "copying");
            return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
        }

        if (HiveUtils.isPartitioned(freshSrcTable)) {
            LOG.warn("Source table " + spec + " is a partitioned table, so " +
                    "not copying");
            return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
        }

        // Check the table that exists already in the destination cluster
        Table existingTable = destMs.getTable(spec.getDbName(),
                spec.getTableName());

        if (existingTable != null) {
            LOG.info("Table " + spec + " exists on destination");
            objectConflictHandler.handleCopyConflict(srcCluster, destCluster,
                    freshSrcTable, existingTable);
        }

        Table destTable = objectModifier.createDestTable(
                srcCluster,
                destCluster,
                freshSrcTable,
                existingTable);

        // Refresh in case the conflict handler did something
        existingTable = destMs.getTable(spec.getDbName(),
                spec.getTableName());

        // Copy HDFS data if the location has changed in the destination object.
        // Usually, this is the case, but for S3 backed tables, the location
        // doesn't change.
        boolean locationDefined =
                ReplicationUtils.getLocation(freshSrcTable) != null;

        Path srcPath = new Path(
                freshSrcTable.getSd().getLocation());
        Path destPath = new Path(destTable.getSd().getLocation());

        boolean needToCopy = locationDefined &&
                !ReplicationUtils.getLocation(freshSrcTable).equals(
                        ReplicationUtils.getLocation(destTable)) &&
                !directoryCopier.equalDirs(srcPath, destPath);

        long bytesCopied = 0;

        if (needToCopy) {
            if (!allowDataCopy) {
                LOG.debug(String.format("Need to copy %s to %s, but data " +
                        "copy is not allowed",
                        srcPath,
                        destPath));
                return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
            }
            Random random = new Random();
            long randomLong = random.nextLong();

            // Copy directory
            bytesCopied = directoryCopier.copy(srcPath, destPath,
                    Arrays.asList(
                            srcCluster.getName(),
                            spec.getDbName(),
                            spec.getTableName()));
        } else {
            LOG.info("Not copying data");
        }

        // Figure out what to do with the table
        MetadataAction action = MetadataAction.NOOP;
        if (existingTable == null) {
            action = MetadataAction.CREATE;
        } else if (!ReplicationUtils.stripNonComparables(existingTable)
                .equals(ReplicationUtils.stripNonComparables(destTable))) {
            action = MetadataAction.ALTER;
        }

        switch (action) {

            case CREATE:
                LOG.info("Creating " + spec + " since it does not exist on " +
                        "the destination");
                ReplicationUtils.createDbIfNecessary(srcMs, destMs,
                        destTable.getDbName());
                LOG.info("Creating: " + destTable);
                destMs.createTable(destTable);
                LOG.info("Successfully created " + spec);
                break;

            case ALTER:
                LOG.info("Altering table " + spec + " on destination");
                LOG.info("Existing table: " + existingTable);
                LOG.info("New table: " + destTable);
                destMs.alterTable(destTable.getDbName(),
                        destTable.getTableName(),
                        destTable);
                LOG.info("Successfully altered " + spec);
                break;

            case NOOP:
                LOG.info("Destination table is up to date - not doing " +
                        "anything for " + spec);
                break;
        }

        return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, bytesCopied);
    }

    @Override
    public LockSet getRequiredLocks() {
        LockSet lockSet = new LockSet();
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
        // TODO: Lock the location
        return lockSet;
    }
}
