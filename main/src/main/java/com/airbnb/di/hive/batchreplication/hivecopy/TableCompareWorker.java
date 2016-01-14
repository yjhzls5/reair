package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.HardCodedCluster;
import com.airbnb.di.hive.replication.deploy.ConfigurationException;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;
import com.airbnb.di.hive.replication.primitives.TaskEstimator;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Mapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob.serializeJobResult;
import static com.airbnb.di.hive.replication.deploy.ReplicationLauncher.makeURI;

/**
 * Worker to compare table entity
 */
public class TableCompareWorker {
    private static class BlackListPair {
        private final Pattern dbNamePattern;
        private final Pattern tblNamePattern;

        public BlackListPair(String dbNamePattern, String tblNamePattern) {
            this.dbNamePattern = Pattern.compile(dbNamePattern);
            this.tblNamePattern = Pattern.compile(tblNamePattern);
        }

        boolean matches(String dbName, String tableName) {
            Matcher dbMatcher = this.dbNamePattern.matcher(dbName);
            Matcher tblmatcher = this.tblNamePattern.matcher(tableName);
            return dbMatcher.matches() && tblmatcher.matches();
        }
    }

    private static final DestinationObjectFactory destinationObjectFactory = new DestinationObjectFactory();

    private Configuration conf;
    private HiveMetastoreClient srcClient;
    private HiveMetastoreClient dstClient;
    private Cluster srcCluster;
    private Cluster dstCluster;
    // list of db and table blacklist.
    private List<BlackListPair> blackList;
    private DirectoryCopier directoryCopier;
    private TaskEstimator estimator;

    protected void setup(Mapper.Context context) throws IOException, InterruptedException, ConfigurationException {
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

            if (context.getConfiguration().get(DeployConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST) == null) {
                this.blackList = Collections.<BlackListPair>emptyList();

            } else {
                this.blackList = Lists.transform(Arrays.asList(context.getConfiguration().
                                get(DeployConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST).split(",")),
                        new Function<String, BlackListPair>() {
                            @Override
                            public BlackListPair apply(@Nullable String s) {
                                String[] parts = s.split(":");
                                return new BlackListPair(parts[0], parts[1]);
                            }
                        });
            }

            this.directoryCopier = new DirectoryCopier(conf, srcCluster.getTmpDir(), true);
            this.estimator = new TaskEstimator(conf,
                    destinationObjectFactory,
                    srcCluster,
                    dstCluster,
                    directoryCopier);
        } catch (HiveMetastoreException e) {
            throw new IOException(e);
        }
    }

    protected List<String> processTable(final String db, final String table)
            throws IOException, HiveMetastoreException {
        // If table and db matches black list, we will skip it.
        if (Iterables.any(blackList,
                new Predicate<BlackListPair>() {
                    @Override
                    public boolean apply(@Nullable BlackListPair s) {
                        return s.matches(db, table);
                    }
                })) {
            return Collections.emptyList();
        }

        HiveObjectSpec spec = new HiveObjectSpec(db, table);

        // Table exists in source, but not in dest. It should copy the table.
        TaskEstimate estimate = estimator.analyze(spec);
        ArrayList<String> ret = new ArrayList<>();

        ret.add(serializeJobResult(estimate, spec));

        Table tab = srcClient.getTable(db, table);
        if (tab != null && tab.getPartitionKeys().size() > 0) {
            // partition tables need to generate partitions.
            HashSet<String> partNames = Sets.newHashSet(srcClient.getPartitionNames(db, table));
            HashSet<String> dstPartNames = Sets.newHashSet(dstClient.getPartitionNames(db, table));
            ret.addAll(Lists.transform(Lists.newArrayList(Sets.union(partNames, dstPartNames)),
                    new Function<String, String>() {
                        public String apply(String s) {
                            return serializeJobResult(new TaskEstimate(TaskEstimate.TaskType.CHECK_PARTITION,
                                                                        false,
                                                                        false,
                                                                        Optional.empty(),
                                                                        Optional.empty()),
                                                      new HiveObjectSpec(db, table, s));
                        }
                    }));
        }

        return ret;
    }

    protected void cleanup() throws IOException, InterruptedException {
        this.srcClient.close();
        this.dstClient.close();
    }
}
