package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.HardCodedCluster;
import com.airbnb.di.hive.replication.deploy.ConfigurationException;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
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
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.genValue;
import static com.airbnb.di.hive.replication.deploy.ReplicationLauncher.makeURI;

/**
 */
public class TableCompareWorker {
    static class BlackListPair {
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

    private HiveMetastoreClient srcClient;
    private HiveMetastoreClient dstClient;
    private Cluster srcCluster;
    private Cluster dstCluster;
    // list of db and table blacklist.
    private List<BlackListPair> blackList;

    private boolean allowS3TableSync;

    protected void setup(Mapper.Context context) throws IOException, InterruptedException, ConfigurationException {
        try {
            Configuration conf = context.getConfiguration();
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

            this.blackList = Lists.transform(Arrays.asList(context.getConfiguration().
                    get(MetastoreReplicationJob.REPLICATION_METASTORE_BLACKLIST).split(",")),
                    new Function<String, BlackListPair>() {
                        @Override
                        public BlackListPair apply(@Nullable String s) {
                            String[] parts = s.split(":");
                            return new BlackListPair(parts[0], parts[1]);
                        }
                    });


            this.allowS3TableSync = context.getConfiguration().getBoolean(MetastoreReplicationJob.REPLICATION_ALLOW_S3, false);
        } catch (HiveMetastoreException e) {
            throw new IOException(e);
        }
    }

    protected List<String> processTable(final String db, final String table)
            throws HiveMetastoreException {
        // If table and db matches black list, we will skip it.
        if (Iterables.any(blackList,
                new Predicate<BlackListPair>() {
                    @Override
                    public boolean apply(@Nullable BlackListPair s) {
                        return s.matches(db, table);
                    }
                })) {
            return ImmutableList.of(genValue(MetastoreReplicationJob.MetastoreAction.SKIP_TABLE.name(), db, table, "source blacklisted"));
        }

        Table tab = srcClient.getTable(db, table);
        Table dstTab = dstClient.getTable(db, table);
        if (tab == null) {
            return ImmutableList.of(genValue(MetastoreReplicationJob.MetastoreAction.SKIP_TABLE.name(), db, table, "source deleted"));
        } else {
            String srcLocation = tab.getSd().getLocation();
            if (!tab.getTableType().contains("VIEW")) {
                HdfsPath ret = new HdfsPath(srcLocation);
                if (!allowS3TableSync && ret.getProto().contains("s3")) {
                    return ImmutableList.of(
                            genValue(MetastoreReplicationJob.MetastoreAction.SKIP_TABLE.name(), db, table,
                                    "s3 backed table skipped"));
                } else {
                    if (!ret.getHost().matches(MetastoreReplicationJob.HDFSPATH_CHECK_MAP.get(this.metastore)) &&
                            !ret.getProto().contains("s3")) {
                        return ImmutableList.of(
                                genValue(MetastoreReplicationJob.MetastoreAction.SKIP_TABLE.name(), db, table,
                                        "table is from different cluster"));
                    }
                }
            }

            ArrayList<String> ret = new ArrayList<>();
            if (dstTab == null) {
                if (tab.getPartitionKeys().size() > 0) {
                    ret.add(genValue(
                            MetastoreReplicationJob.MetastoreAction.CREATE_TABLE.name(), db, table, tab.getSd().getLocation()));

                    ret.addAll(
                            Lists.transform(srcClient.getPartitionNames(db, table), new Function<String, String>() {
                                public String apply(String s) {
                                    return genValue(MetastoreReplicationJob.MetastoreAction.CREATE_PARTITION.name(), db, table, s);
                                }
                            }));
                } else {
                    if (tab.getTableType().contains("VIEW")) {
                        ret.add(genValue(MetastoreReplicationJob.MetastoreAction.CREATE_TABLE.name(), db, table, ""));
                    } else {
                        ret.add(genValue(MetastoreReplicationJob.MetastoreAction.CREATE_TABLE_NONPART.name(), db, table,
                                tab.getSd().getLocation()));
                    }
                }
            } else {
                MetastoreCompareUtils.UpdateAction action = MetastoreCompareUtils.CompareTableForReplication(tab, dstTab);
                switch (action) {
                case UPDATE:
                    ret.add(genValue(MetastoreReplicationJob.MetastoreAction.UPDATE_TABLE.name(), db, table, "table def changed"));
                    break;
                case RECREATE:
                    // work around for hive bug that can not drop view
                    if (tab.getTableType().contains("VIEW")) {
                        ret.add(genValue(MetastoreReplicationJob.MetastoreAction.UPDATE_TABLE.name(), db, table, "view def changed"));
                    } else {
                        ret.add(genValue(MetastoreReplicationJob.MetastoreAction.RECREATE_TABLE.name(), db, table, "table def changed"));
                    }
                    break;
                case NO_CHANGE:
                    if (!dstTab.getParameters().containsKey(MetastoreReplicationJob.REPLICATED_FROM_PINKY_BRAIN)) {
                        ret.add(genValue(MetastoreReplicationJob.MetastoreAction.UPDATE_TABLE.name(), db, table,
                                "mark table replicated from pinky/brain"));
                    } else {
                        ret.add(genValue(MetastoreReplicationJob.MetastoreAction.SAME_META_TABLE.name(), db, table,
                                "table def not changed"));
                    }
                    break;
                default:
                    throw new HiveMetastoreException(new InterruptedException("invalid table compare result"));
                }

                if (tab.getPartitionKeys().size() > 0) {
                    HashSet<String> partNames = Sets.newHashSet(srcClient.getPartitionNames(db, table));
                    HashSet<String> dstPartNames = Sets.newHashSet(dstClient.getPartitionNames(db, table));
                    ret.addAll(Lists.transform(Lists.newArrayList(Sets.difference(partNames, dstPartNames)),
                            new Function<String, String>() {
                                public String apply(String s) {
                                    return genValue(MetastoreReplicationJob.MetastoreAction.CREATE_PARTITION.name(), db, table, s);
                                }
                            }));
                    ret.addAll(Lists.transform(Lists.newArrayList(Sets.difference(dstPartNames, partNames)),
                            new Function<String, String>() {
                                public String apply(String s) {
                                    return genValue(MetastoreReplicationJob.MetastoreAction.DROP_PARTITION.name(), db, table, s);
                                }
                            }));
                    ret.addAll(Lists.transform(Lists.newArrayList(Sets.intersection(partNames, dstPartNames)),
                            new Function<String, String>() {
                                public String apply(String s) {
                                    return genValue(MetastoreReplicationJob.MetastoreAction.UPDATE_PARTITION.name(), db, table, s);
                                }
                            }));
                } else if (!tab.getTableType().contains("VIEW")) {
                    ret.add(genValue(MetastoreReplicationJob.MetastoreAction.CHECK_DIRECTORY_TABLE.name(), db, table,
                            "check directory for non-partition table"));
                }
            }
            return ret;
        }
    }

    protected void cleanup() throws IOException, InterruptedException {
        this.srcClient.close();
        this.dstClient.close();
    }
}
