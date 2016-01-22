package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.ClusterFactory;
import com.airbnb.di.hive.replication.configuration.ConfigurationException;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;
import com.airbnb.di.hive.replication.primitives.TaskEstimator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob.deseralizeJobResult;
import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob.serializeJobResult;

/**
 * Reducer to compare partition entity.
 *
 * Partition entities are evenly distributed using shuffle. The reducer will figure out action for each partition.
 * The reducer will pass through the action for each table entity and append partition action as output for the stage1
 * job.
 */
public class PartitionCompareReducer extends Reducer<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(PartitionCompareReducer.class);

    private static final DestinationObjectFactory destinationObjectFactory = new DestinationObjectFactory();

    private Configuration conf;
    private HiveMetastoreClient srcClient;
    private HiveMetastoreClient dstClient;
    private Cluster srcCluster;
    private Cluster dstCluster;
    private DirectoryCopier directoryCopier;
    private long count = 0;
    private TaskEstimator estimator;

    public PartitionCompareReducer() {
    }

    protected void setup(Context context) throws IOException,
            InterruptedException {
        try {
            this.conf = context.getConfiguration();

            ClusterFactory clusterFactory = MetastoreReplUtils.createClusterFactory(conf);

            this.srcCluster = clusterFactory.getSrcCluster();
            this.srcClient = this.srcCluster.getMetastoreClient();

            this.dstCluster = clusterFactory.getDestCluster();
            this.dstClient = this.dstCluster.getMetastoreClient();

            this.directoryCopier = clusterFactory.getDirectoryCopier();

            this.estimator = new TaskEstimator(conf,
                    destinationObjectFactory,
                    srcCluster,
                    dstCluster,
                    directoryCopier);
        } catch (HiveMetastoreException | ConfigurationException e) {
            throw new IOException(e);
        }
    }

    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            Pair<TaskEstimate, HiveObjectSpec> input = deseralizeJobResult(value.toString());
            TaskEstimate estimate = input.getLeft();
            HiveObjectSpec spec = input.getRight();
            String result = value.toString();
            String extra = "";

            try {
                if (estimate.getTaskType() == TaskEstimate.TaskType.CHECK_PARTITION) {
                    // Table exists in source, but not in dest. It should copy the table.
                    TaskEstimate newEstimate = estimator.analyze(spec);

                    result = serializeJobResult(newEstimate, spec);
                }
            } catch (HiveMetastoreException e) {
                LOG.error(String.format("Hit exception during db:%s, tbl:%s, part:%s", spec.getDbName(),
                        spec.getTableName(), spec.getPartitionName()), e);
                extra = String.format("exception in %s of mapper = %s", estimate.getTaskType().toString(),
                        context.getTaskAttemptID().toString());
            }

            context.write(new Text(result), new Text(extra));
            ++this.count;
            if (this.count % 100 == 0) {
                LOG.info("Processed " + this.count + " entities");
            }
        }
    }

    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        this.srcClient.close();
        this.dstClient.close();
    }
}
