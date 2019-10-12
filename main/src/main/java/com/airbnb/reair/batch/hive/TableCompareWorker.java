package com.airbnb.reair.batch.hive;

import com.airbnb.reair.batch.BatchUtils;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.incremental.primitives.CopyPartitionedTableTask;
import com.airbnb.reair.incremental.primitives.TaskEstimate;
import com.airbnb.reair.incremental.primitives.TaskEstimator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Worker to figure out the action for a table entity.
 *
 * <p>For partitioned table, the worker will generate a CHECK_PARTITION action for each partition.
 * In PartitionCompareReducer, a more specific action will be determined. The reason for having
 * separate table and partition checks is for load balancing. In a production data warehouse,
 * tables can have millions of partitions. Since each check to metastore takes a hundred
 * milliseconds, it is important to distribute metastore calls to many reducers through a shuffle.
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

  private static final DestinationObjectFactory DESTINATION_OBJECT_FACTORY =
      new DestinationObjectFactory();

  private Configuration conf;
  private HiveMetastoreClient srcClient;
  private HiveMetastoreClient dstClient;
  private Cluster srcCluster;
  private Cluster dstCluster;
  // list of db and table blacklist.
  private List<BlackListPair> blackList;

  private DirectoryCopier directoryCopier;
  private TaskEstimator estimator;
  private ObjectConflictHandler objectConflictHandler = new ObjectConflictHandler();

  protected void setup(Mapper.Context context)
      throws IOException, InterruptedException, ConfigurationException {
    try {
      this.conf = context.getConfiguration();
      ClusterFactory clusterFactory = MetastoreReplUtils.createClusterFactory(conf);

      this.srcCluster = clusterFactory.getSrcCluster();
      this.srcClient = this.srcCluster.getMetastoreClient();

      this.dstCluster = clusterFactory.getDestCluster();
      this.dstClient = this.dstCluster.getMetastoreClient();

      this.directoryCopier = clusterFactory.getDirectoryCopier();




      if (context.getConfiguration()
              .get(ConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST) == null) {
        this.blackList = Collections.<BlackListPair>emptyList();

      } else {
        this.blackList = Lists.transform(Arrays.asList(context.getConfiguration()
              .get(ConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST).split(",")),
            new Function<String, BlackListPair>() {
              @Override
              public BlackListPair apply(@Nullable String str) {
                String[] parts = str.split(":");
                return new BlackListPair(parts[0], parts[1]);
              }
            });
      }

      // dbmap
      Map<String,String> dbMap = BatchUtils.getDBMap(context.getConfiguration()) ;
      // TODO: 2019/10/9 dest new db
      DESTINATION_OBJECT_FACTORY.setDbMap(dbMap);

      this.estimator = new TaskEstimator(conf,
          DESTINATION_OBJECT_FACTORY,
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
            public boolean apply(@Nullable BlackListPair blackListPair) {
              return blackListPair.matches(db, table);
            }
          })) {
      return Collections.emptyList();
    }

    HiveObjectSpec spec = new HiveObjectSpec(db, table);


    // Table exists in source, but not in dest. It should copy the table.
    TaskEstimate estimate = estimator.analyze(spec);
    ArrayList<String> ret = new ArrayList<>();

    ret.add(MetastoreReplicationJob.serializeJobResult(estimate, spec));

    Table tab = srcClient.getTable(db, table);
    if (tab != null && tab.getPartitionKeys().size() > 0) {
      // For partitioned table, if action is COPY we need to make sure to handle partition key
      // change case first. The copy task will be run twice once here and the other time at commit
      // phase. The task will handle the case properly.
      if (estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITIONED_TABLE) {
        CopyPartitionedTableTask copyPartitionedTableTaskJob = new CopyPartitionedTableTask(
            conf,
            DESTINATION_OBJECT_FACTORY,
            objectConflictHandler,
            srcCluster,
            dstCluster,
            spec,
            Optional.<Path>empty());
        // deal metastore, if necessary ,create or alter the table
        copyPartitionedTableTaskJob.runTask();
      }

      // partition tables need to generate partitions.
      HashSet<String> partNames = Sets.newHashSet(srcClient.getPartitionNames(db, table));

      // TODO: 2019/10/8 change to new db ;
      HashSet<String> dstPartNames = Sets.newHashSet(dstClient.getPartitionNames(
              DESTINATION_OBJECT_FACTORY.modifyDestDb(db),
              table));

      ret.addAll(Lists.transform(Lists.newArrayList(Sets.union(partNames, dstPartNames)),
            new Function<String, String>() {
              public String apply(String str) {
                return MetastoreReplicationJob.serializeJobResult(
                    new TaskEstimate(TaskEstimate.TaskType.CHECK_PARTITION,
                      false,
                      false,
                      Optional.empty(),
                      Optional.empty()),
                    new HiveObjectSpec(db, table, str));
              }
            }));
    }

    return ret;
  }


  /**
   * process db's all tables
   * @param dbName dbname
   * @return
   * @throws IOException
   * @throws HiveMetastoreException
   */
  protected List<String> processDb(final String dbName )
          throws IOException, HiveMetastoreException {

    List<String> result = Lists.newArrayList();

    List<String> dbTables = this.srcClient.getAllTables(dbName) ;
    if(dbTables != null ){
      for(String tableName : dbTables){
        result.addAll(this.processTable(dbName, tableName) );
      }
    }

    return result;

  }



  protected void cleanup() throws IOException, InterruptedException {
    this.srcClient.close();
    this.dstClient.close();
  }
}
