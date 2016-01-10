package com.airbnb.di.hive.replication.primitives;

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
import com.airbnb.di.multiprocessing.Lock;
import com.airbnb.di.multiprocessing.LockSet;
import com.airbnb.di.multiprocessing.ParallelJobExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by paul_yang on 7/10/15.
 */
public class CopyCompleteTableTask implements ReplicationTask {

    private static final Log LOG = LogFactory.getLog(
            CopyCompleteTableTask.class);

    private Configuration conf;
    private DestinationObjectFactory objectModifier;
    private ObjectConflictHandler objectConflictHandler;
    private Cluster srcCluster;
    private Cluster destCluster;
    private HiveObjectSpec spec;
    private Optional<Path> tableLocation;
    private ParallelJobExecutor copyPartitionsExecutor;
    private DirectoryCopier directoryCopier;

    public CopyCompleteTableTask(Configuration conf,
                                 DestinationObjectFactory objectModifier,
                                 ObjectConflictHandler objectConflictHandler,
                                 Cluster srcCluster,
                                 Cluster destCluster,
                                 HiveObjectSpec spec,
                                 Optional<Path> tableLocation,
                                 ParallelJobExecutor copyPartitionsExecutor,
                                 DirectoryCopier directoryCopier) {
        this.conf = conf;
        this.objectModifier = objectModifier;
        this.objectConflictHandler = objectConflictHandler;
        this.srcCluster = srcCluster;
        this.destCluster = destCluster;
        this.spec = spec;
        this.tableLocation = tableLocation;
        this.copyPartitionsExecutor = copyPartitionsExecutor;
        this.directoryCopier = directoryCopier;
    }

    public RunInfo runTask() throws DistCpException, HiveMetastoreException,
            IOException {
        LOG.debug("Copying " + spec);

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
            LOG.debug("Source table " + spec + " is a partitioned table");

            // Create a collection containing all the partitions that should
            // be copied.
            List<String> partitionNames = srcMs.getPartitionNames(
                    spec.getDbName(),
                    spec.getTableName());
            Map<HiveObjectSpec, Partition> specToPartition =
                    new HashMap<HiveObjectSpec, Partition>();
            for (String partitionName : partitionNames) {
                Partition partition = srcMs.getPartition(spec.getDbName(),
                        spec.getTableName(), partitionName);
                HiveObjectSpec partitionSpec = new HiveObjectSpec(
                        spec.getDbName(), spec.getTableName(),
                        partitionName);
                specToPartition.put(partitionSpec, partition);
            }

            Optional<Path> commonDirectory = Optional.empty();

            if (specToPartition.size() > 0) {
                commonDirectory = CopyPartitionsTask.findCommonDirectory(
                        spec, specToPartition);
            }

            CopyPartitionsTask job = new CopyPartitionsTask(conf,
                    objectModifier,
                    objectConflictHandler,
                    srcCluster,
                    destCluster,
                    spec,
                    partitionNames,
                    commonDirectory,
                    copyPartitionsExecutor,
                    directoryCopier);

            RunInfo copyPartitionsRunInfo = job.runTask();

            if (copyPartitionsRunInfo.getRunStatus() !=
                    RunInfo.RunStatus.SUCCESSFUL ||
                    copyPartitionsRunInfo.getRunStatus() !=
                            RunInfo.RunStatus.NOT_COMPLETABLE) {
                return copyPartitionsRunInfo;
            }

            CopyPartitionedTableTask copyTableTask = new
                    CopyPartitionedTableTask(
                    conf,
                    objectModifier,
                    objectConflictHandler,
                    srcCluster,
                    destCluster,
                    spec,
                    commonDirectory);
            RunInfo copyTableRunInfo = copyTableTask.runTask();

            return new RunInfo(copyTableRunInfo.getRunStatus(),
                    copyPartitionsRunInfo.getBytesCopied() +
                            copyTableRunInfo.getBytesCopied());
        } else {
            LOG.debug("Source table " + spec + " is an unpartitioned table");
            CopyUnpartitionedTableTask copyJob = new CopyUnpartitionedTableTask(
                    conf,
                    objectModifier,
                    objectConflictHandler,
                    srcCluster,
                    destCluster,
                    spec,
                    tableLocation,
                    directoryCopier,
                    true);
            return copyJob.runTask();
        }
    }

    @Override
    public LockSet getRequiredLocks() {
        LockSet lockSet = new LockSet();
        lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
        return lockSet;
    }
}
