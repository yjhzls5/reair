package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.HardCodedCluster;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.deploy.ConfigurationException;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import com.airbnb.di.hive.replication.primitives.CopyPartitionTask;
import com.airbnb.di.hive.replication.primitives.CopyPartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.CopyUnpartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob.deseralizeJobResult;
import static com.airbnb.di.hive.replication.deploy.ReplicationLauncher.makeURI;

/**
 * Stage 3 mapper for commit copy action
 */
public class Stage3CommitChangeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(Stage3CommitChangeMapper.class);
    private static final DestinationObjectFactory DESTINATION_OBJECT_FACTORY = new DestinationObjectFactory();

    private Configuration conf;
    private HiveMetastoreClient srcClient;
    private HiveMetastoreClient dstClient;
    private Cluster srcCluster;
    private Cluster dstCluster;
    private DirectoryCopier directoryCopier;
    private ObjectConflictHandler objectConflictHandler = new ObjectConflictHandler();

    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            this.conf = context.getConfiguration();
            // Create the source cluster object
            String srcClusterName = conf.get(
                    DeployConfigurationKeys.SRC_CLUSTER_NAME);
            String srcMetastoreUrlString = conf.get(
                    DeployConfigurationKeys.SRC_CLUSTER_METASTORE_URL);
            URI srcMetastoreUrl = makeURI(srcMetastoreUrlString);
            String srcHdfsRoot = conf.get(
                    DeployConfigurationKeys.SRC_HDFS_ROOT);
            String srcHdfsTmp = conf.get(
                    DeployConfigurationKeys.SRC_HDFS_TMP);
            this.srcCluster = new HardCodedCluster(
                    srcClusterName,
                    srcMetastoreUrl.getHost(),
                    srcMetastoreUrl.getPort(),
                    null,
                    null,
                    new Path(srcHdfsRoot),
                    new Path(srcHdfsTmp));
            this.srcClient = this.srcCluster.getMetastoreClient();

            // Create the dest cluster object
            String destClusterName = conf.get(
                    DeployConfigurationKeys.DEST_CLUSTER_NAME);
            String destMetastoreUrlString = conf.get(
                    DeployConfigurationKeys.DEST_CLUSTER_METASTORE_URL);
            URI destMetastoreUrl = makeURI(destMetastoreUrlString);
            String destHdfsRoot = conf.get(
                    DeployConfigurationKeys.DEST_HDFS_ROOT);
            String destHdfsTmp = conf.get(
                    DeployConfigurationKeys.DEST_HDFS_TMP);
            this.dstCluster = new HardCodedCluster(
                    destClusterName,
                    destMetastoreUrl.getHost(),
                    destMetastoreUrl.getPort(),
                    null,
                    null,
                    new Path(destHdfsRoot),
                    new Path(destHdfsTmp));
            this.dstClient = this.dstCluster.getMetastoreClient();
            this.directoryCopier = new DirectoryCopier(conf, srcCluster.getTmpDir(), false);
        } catch (HiveMetastoreException | ConfigurationException e) {
            throw new IOException(e);
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Pair<TaskEstimate, HiveObjectSpec> input = deseralizeJobResult(value.toString());
            TaskEstimate estimate = input.getLeft();
            HiveObjectSpec spec = input.getRight();
            RunInfo status = null;

            switch (estimate.getTaskType()) {
            case COPY_PARTITION:
                CopyPartitionTask copyPartitionTask = new CopyPartitionTask(
                        conf,
                        DESTINATION_OBJECT_FACTORY,
                        objectConflictHandler,
                        srcCluster,
                        dstCluster,
                        spec,
                        Optional.<Path>empty(),
                        Optional.<Path>empty(),
                        directoryCopier,
                        false);
                status = copyPartitionTask.runTask();
                context.write(value, new Text(status.getRunStatus().toString()));
                break;
            case COPY_PARTITIONED_TABLE:
                CopyPartitionedTableTask copyPartitionedTableTaskJob = new CopyPartitionedTableTask(
                        conf,
                        DESTINATION_OBJECT_FACTORY,
                        objectConflictHandler,
                        srcCluster,
                        dstCluster,
                        spec,
                        Optional.<Path>empty());
                status = copyPartitionedTableTaskJob.runTask();
                context.write(value, new Text(status.getRunStatus().toString()));
                break;
            case COPY_UNPARTITIONED_TABLE:
                // Replicate the table
                CopyUnpartitionedTableTask copyUnpartitionedTableTask = new CopyUnpartitionedTableTask(
                        conf,
                        DESTINATION_OBJECT_FACTORY,
                        objectConflictHandler,
                        srcCluster,
                        dstCluster,
                        spec,
                        Optional.<Path>empty(),
                        directoryCopier,
                        false);
                status = copyUnpartitionedTableTask.runTask();
                context.write(value, new Text(status.getRunStatus().toString()));
                break;
            case DROP_PARTITION:
                Partition dstPart = dstClient.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());
                if (dstPart == null) {
                    context.write(value, new Text("partition already dropped"));
                    break;
                }

                HdfsPath partHdfsPath = new HdfsPath(dstPart.getSd().getLocation());
                if (partHdfsPath.getProto().contains("s3")) {
                    context.write(value, new Text("s3 backed partition drop skipped"));
                } else {
                    try {
                        dstClient.dropPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName(), true);
                        context.write(value, new Text("partition dropped"));
                    } catch (HiveMetastoreException e) {
                        LOG.info("failed to drop partition." + e.getMessage());
                        context.write(value, new Text("drop partition failed"));
                    }
                }
                break;
            case DROP_TABLE:
                Table dstTable = dstClient.getTable(spec.getDbName(), spec.getTableName());
                if (dstTable == null) {
                    context.write(value, new Text("table already dropped"));
                    break;
                }

                HdfsPath tblHdfsPath = new HdfsPath(dstTable.getSd().getLocation());
                if (tblHdfsPath.getProto().contains("s3")) {
                    context.write(value, new Text("s3 backed table drop skipped"));
                } else {
                    try {
                        dstClient.dropTable(spec.getDbName(), spec.getTableName(), true);
                        context.write(value, new Text("table dropped"));
                    } catch (HiveMetastoreException e) {
                        LOG.info("failed to drop table." + e.getMessage());
                        context.write(value, new Text("drop table failed"));
                    }
                }
                break;
            default:
                break;
            }
        } catch (HiveMetastoreException | DistCpException e) {
            LOG.info(String.format("%s got exception", value.toString()));
            LOG.info(e.getMessage());
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.srcClient.close();
        this.dstClient.close();
    }
}
