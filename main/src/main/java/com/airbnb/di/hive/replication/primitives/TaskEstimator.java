package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveUtils;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Optional;

/**
 * Given a Hive object spec, this class tries to figure out what operations
 * would be necessary to replicate the object from the source cluster to the
 * destination cluster. If the source object does not exist, but the destination
 * does, a drop is assumed to be necessary.
 */
public class TaskEstimator {
    private static final Log LOG = LogFactory.getLog(
            TaskEstimator.class);

    private Configuration conf;
    private DestinationObjectFactory destObjectFactory;
    private Cluster srcCluster;
    private Cluster destCluster;
    private DirectoryCopier directoryCopier;

    public TaskEstimator(Configuration conf,
                         DestinationObjectFactory destObjectFactory,
                         Cluster srcCluster,
                         Cluster destCluster,
                         DirectoryCopier directoryCopier) {
        this.conf = conf;
        this.destObjectFactory = destObjectFactory;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.directoryCopier = directoryCopier;
    }

    /**
     * Returns an estimate of what kind of task should be run to replicate the
     * given object.
     *
     * @param spec The Hive object that should be replicated from the source to
     *             the destination.
     * @throws HiveMetastoreException
     * @throws IOException
     */
    public TaskEstimate analyze(HiveObjectSpec spec)
            throws HiveMetastoreException, IOException {
        if (!spec.isPartition()) {
            return analyzeTableSpec(spec);
        } else {
            return analyzePartitionSpec(spec);
        }
    }

    private TaskEstimate analyzeTableSpec(HiveObjectSpec spec)
            throws HiveMetastoreException, IOException {
        if (spec.isPartition()) {
            throw new RuntimeException("Argument should be a table " +
                    spec);
        }

        HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();
        Table tableOnSrc = srcMs.getTable(spec.getDbName(),
                spec.getTableName());

        HiveMetastoreClient destMs = destCluster.getMetastoreClient();
        Table tableOnDest = destMs.getTable(spec.getDbName(),
                spec.getTableName());

        // If the souce table doesn't exist but the destination table doesn't,
        // then it's most likely a drop.
        if (tableOnSrc == null && tableOnDest != null) {
            return new TaskEstimate(TaskEstimate.TaskType.DROP_TABLE,
                    false,
                    false,
                    null,
                    null);
        }

        // Nothing to do if the source table doesn't exist
        if (tableOnSrc == null) {
            return new TaskEstimate(TaskEstimate.TaskType.NO_OP,
                    false,
                    false,
                    null,
                    null);
        }

        boolean isPartitionedTable = HiveUtils.isPartitioned(tableOnSrc);

        // See if we need to update the data
        // Locations are not defined for views
        boolean updateData = false;
        Optional<Path> srcPath = ReplicationUtils.getLocation(tableOnSrc);

        Table expectedDestTable = destObjectFactory.createDestTable(
                srcCluster,
                destCluster,
                tableOnSrc,
                tableOnDest);

        Optional<Path> destPath = ReplicationUtils.getLocation(
                expectedDestTable);

        if (!isPartitionedTable &&
                srcPath.isPresent() &&
                !srcPath.equals(destPath)) {
            updateData = !directoryCopier.equalDirs(srcPath.get(),
                    destPath.get());
        }

        // See if we need to update the metadata
        boolean updateMetadata = tableOnDest == null ||
                !ReplicationUtils.stripNonComparables(tableOnDest)
                        .equals(ReplicationUtils.stripNonComparables(
                                expectedDestTable));

        if (!updateData && !updateMetadata) {
            return new TaskEstimate(TaskEstimate.TaskType.NO_OP,
                    false,
                    false,
                    null,
                    null);
        } else if (!isPartitionedTable) {
            return new TaskEstimate(
                    TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE,
                    updateMetadata,
                    updateData,
                    srcPath.get(),
                    destPath.get());
        } else {
            return new TaskEstimate(
                    TaskEstimate.TaskType.COPY_PARTITIONED_TABLE,
                    true,
                    false,
                    null,
                    null);
        }
    }

    private TaskEstimate analyzePartitionSpec(HiveObjectSpec spec)
            throws HiveMetastoreException, IOException {

        if (!spec.isPartition()) {
            throw new RuntimeException("Argument should be a partition " +
                    spec);
        }
        boolean updateData = false;

        HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();
        Partition partitionOnSrc = srcMs.getPartition(spec.getDbName(),
                spec.getTableName(), spec.getPartitionName());

        HiveMetastoreClient destMs = destCluster.getMetastoreClient();
        Partition partitionOnDest = destMs.getPartition(spec.getDbName(),
                spec.getTableName(), spec.getPartitionName());

        // If the source partition does not exist, but the destination does,
        // it's most likely a drop.
        if (partitionOnSrc == null && partitionOnDest != null) {
            return new TaskEstimate(TaskEstimate.TaskType.DROP_PARTITION,
                    false,
                    false,
                    null,
                    null);
        }

        if (partitionOnSrc == null) {
            return new TaskEstimate(TaskEstimate.TaskType.NO_OP,
                    false,
                    false,
                    null,
                    null);
        }

        Partition expectedDestPartition = destObjectFactory.createDestPartition(
                srcCluster,
                destCluster,
                partitionOnSrc,
                partitionOnDest);

        Optional<Path> srcPath = ReplicationUtils.getLocation(partitionOnSrc);
        Optional<Path> destPath =
                ReplicationUtils.getLocation(expectedDestPartition);

        // See if we need to update the data
        if (srcPath.isPresent() &&
                !srcPath.equals(destPath)) {
                updateData = !directoryCopier.equalDirs(srcPath.get(),
                        destPath.get());
        }

        // A metadata update is required if the destination partition doesn't
        // exist or the metadata differs from what's expected.
        boolean updateMetadata = partitionOnDest == null ||
                !ReplicationUtils.stripNonComparables(partitionOnDest)
                        .equals(ReplicationUtils.stripNonComparables(
                                expectedDestPartition));

        if (!updateData && !updateMetadata) {
            return new TaskEstimate(TaskEstimate.TaskType.NO_OP,
                    false,
                    false,
                    null,
                    null);
        } else {
            return new TaskEstimate(TaskEstimate.TaskType.COPY_PARTITION,
                    updateMetadata,
                    updateData,
                    srcPath.get(),
                    destPath.orElse(null));
        }
    }
}
