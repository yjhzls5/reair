package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.FsUtils;
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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Arrays;

/**
 * Known 'issue': if multiple copy partition jobs are kicked off, and the
 * partitioned table doesn't exist on the destination, then it's possible that
 * multiple copy partition jobs try to create the same partitioned table.
 *
 * This can result in a failure, but should be corrected on a retry.
 */

public class CopyPartitionTask implements ReplicationTask {

    private static final Log LOG = LogFactory.getLog(CopyPartitionTask.class);

    private Configuration conf;
    private DestinationObjectFactory destObjectFactory;
    private ObjectConflictHandler objectConflictHandler;
    private Cluster srcCluster;
    private Cluster destCluster;
    private HiveObjectSpec spec;
    private Path partitionLocation;
    private Path optimisticCopyRoot;
    private DirectoryCopier directoryCopier;


    public CopyPartitionTask(Configuration conf,
                             DestinationObjectFactory destObjectFactory,
                             ObjectConflictHandler objectConflictHandler,
                             Cluster srcCluster,
                             Cluster destCluster,
                             HiveObjectSpec spec,
                             Path partitionLocation,
                             Path optimisticCopyRoot,
                             DirectoryCopier directoryCopier) {
        this.conf = conf;
        this.destObjectFactory = destObjectFactory;
        this.objectConflictHandler = objectConflictHandler;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.spec = spec;
        this.partitionLocation = partitionLocation;
        this.optimisticCopyRoot = optimisticCopyRoot;
        this.directoryCopier = directoryCopier;
    }

    public RunInfo runTask() throws HiveMetastoreException, DistCpException,
            IOException {
        LOG.info("Copying " + spec);

        HiveMetastoreClient destMs = destCluster.getMetastoreClient();
        HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

        Partition freshSrcPartition = srcMs.getPartition(spec.getDbName(),
                spec.getTableName(), spec.getPartitionName());

        if (freshSrcPartition == null) {
            LOG.warn("Source partition " + spec + " does not exist, so not " +
                    "copying");
            return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
        }

        // Before copying a partition, first make sure that table is up to date
        Table srcTable = srcMs.getTable(spec.getDbName(), spec.getTableName());
        Table destTable = destMs.getTable(spec.getDbName(),
                spec.getTableName());

        if (srcTable == null) {
            LOG.warn("Source table " + spec + " doesn't exist, so not " +
                    "copying");
            return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
        }

        if (destTable == null ||
                !ReplicationUtils.schemasMatch(srcTable, destTable)) {
            LOG.warn("Copying source table over to the destination since " +
                    "schemas do not match. (source: " + srcTable +
                    " destination: "
                    + destTable + ")");
            CopyPartitionedTableTask copyTableJob =
                    new CopyPartitionedTableTask(conf,
                            destObjectFactory,
                            objectConflictHandler,
                            srcCluster,
                            destCluster,
                            spec.getTableSpec(),
                            ReplicationUtils.getLocation(srcTable));
            RunInfo status = copyTableJob.runTask();
            if (status.getRunStatus() != RunInfo.RunStatus.SUCCESSFUL) {
                LOG.error("Failed to copy " + spec.getTableSpec());
                return new RunInfo(RunInfo.RunStatus.FAILED, 0);
            }
        }

        Partition existingPartition = destMs.getPartition(spec.getDbName(),
                spec.getTableName(), spec.getPartitionName());

        Partition destPartition = destObjectFactory.createDestPartition(
                srcCluster,
                destCluster,
                freshSrcPartition,
                existingPartition);

        if (existingPartition != null) {
            LOG.info("Partition " + spec + " already exists!");
            objectConflictHandler.handleCopyConflict(srcCluster, destCluster,
                    freshSrcPartition, existingPartition);
        }

        // Copy HDFS data
        long bytesCopied = 0;
        boolean locationDefined =
                ReplicationUtils.getLocation(freshSrcPartition) != null;

        // Try to copy data only if the location is defined and the location
        // for the destination object is different. Usually, the location will
        // be different as it will be situated on a different HDFS, but for
        // S3 backed tables, the location may not change.
        boolean needToCopy = false;
        // TODO: An optimization can be made here to check for directories that
        // already match and no longer need to be copied.
        if (locationDefined &&
                !ReplicationUtils.getLocation(freshSrcPartition).equals(
                        ReplicationUtils.getLocation(destPartition))) {
            // If a directory was copied optimistically, check if the data is
            // there. If the data is there and it matches up with what is
            // expected, then the directory can be moved into place.
            if (optimisticCopyRoot != null) {
                Path srcLocation =
                        new Path(freshSrcPartition.getSd().getLocation());

                // Assume that on the source, a table is stored at /a, and the
                // partitions are stored at /a/ds=1 and /a/ds=1.
                //
                // For this optimization, the source directory (/u/a) was
                // copied to a temporary location (/tmp/u/a). The optimistic
                // copy root would be /tmp. To figure out the directory
                // containing a partition's data, start with the optimistic
                // copy root and add the relative path from / - e.g.
                // /tmp + u/a/ds=1 = /tmp/u/a/ds=1
                Path copiedPartitionDataLocation = new Path(optimisticCopyRoot,
                        StringUtils.stripStart(srcLocation.toUri().getPath(),
                                "/"));

                if (directoryCopier.equalDirs(srcLocation,
                        copiedPartitionDataLocation)) {
                    // In this case, the data is there and we can move the
                    // directory to the expected location.
                    Path destinationPath = new Path(
                            destPartition.getSd().getLocation());

                    FsUtils.replaceDirectory(conf, copiedPartitionDataLocation,
                            destinationPath);
                } else {
                    needToCopy = true;
                }
            } else {
                needToCopy = true;
            }
        }

        if (needToCopy) {
            Path srcPath = new Path(freshSrcPartition.getSd().getLocation());
            Path destPath = new Path(destPartition.getSd().getLocation());

            if (!FsUtils.dirExists(conf, srcPath)) {
                LOG.error("Source path " + srcPath + " does not exist!");
                return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
            }

            bytesCopied = directoryCopier.copy(srcPath,
                    destPath,
                    Arrays.asList(
                            srcCluster.getName(),
                            spec.getDbName(),
                            spec.getTableName()));
        }

        // Figure out what to do with the table
        MetadataAction action = MetadataAction.NOOP;
        if (existingPartition == null) {
            // If the partition doesn't exist on the destination, we need to
            // create it.
            action = MetadataAction.CREATE;
        } else if (!ReplicationUtils.stripNonComparables(existingPartition)
                .equals(ReplicationUtils.stripNonComparables(destPartition))) {
            // The partition exists on the destination, but some of the metadata
            // attributes are not as expected. This can be fixed with an alter
            // call.
            action = MetadataAction.ALTER;
        }

        // Take necessary action
        switch (action) {

            case CREATE:
                ReplicationUtils.createDbIfNecessary(srcMs, destMs,
                        destPartition.getDbName());

                LOG.info("Creating " + spec + " since it does not exist on " +
                        "the destination");
                destMs.addPartition(destPartition);
                LOG.info("Successfully created " + spec);
                break;

            case ALTER:
                LOG.info("Altering partition " + spec + " on destination");
                destMs.alterPartition(destPartition.getDbName(),
                        destPartition.getTableName(),
                        destPartition);
                break;

            case NOOP:
                LOG.info("Not doing anything for " + spec);
                break;

            default:
                throw new RuntimeException("Unhandled case!");
        }

        return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, bytesCopied);
    }

    public HiveObjectSpec getSpec() {
        return this.spec;
    }

    @Override
    public LockSet getRequiredLocks() {
        LockSet lockSet = new LockSet();
        lockSet.add(new Lock(Lock.Type.SHARED, spec.getTableName().toString()));
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
        return lockSet;
    }
}
