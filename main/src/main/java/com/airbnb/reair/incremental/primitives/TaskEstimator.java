package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveUtils;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Optional;

/**
 * Given a Hive object spec, this class tries to figure out what operations would be necessary to
 * replicate the object from the source cluster to the destination cluster. If the source object
 * does not exist, but the destination does, a drop is assumed to be necessary.
 */
public class TaskEstimator {
  private static final Log LOG = LogFactory.getLog(TaskEstimator.class);

  private Configuration conf;
  private DestinationObjectFactory destObjectFactory;
  private Cluster srcCluster;
  private Cluster destCluster;
  private DirectoryCopier directoryCopier;

  /**
   * Constructor for a task estimator.
   *
   * @param conf configuration object
   * @param destObjectFactory factory for creating objects for the destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param directoryCopier runs directory copies through MR jobs
   */
  public TaskEstimator(
      Configuration conf,
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
   * Returns an estimate of what kind of task should be run to replicate the given object.
   *
   * @param spec The Hive object that should be replicated from the source to the destination.
   * @throws HiveMetastoreException if there is an error connecting to the metastore
   * @throws IOException if there is an error accessing the filesystem
   */
  public TaskEstimate analyze(HiveObjectSpec spec) throws HiveMetastoreException, IOException {
    if (!spec.isPartition()) {
      return analyzeTableSpec(spec);
    } else {
      return analyzePartitionSpec(spec);
    }
  }

  private TaskEstimate analyzeTableSpec(HiveObjectSpec spec)
      throws HiveMetastoreException, IOException {
    if (spec.isPartition()) {
      throw new RuntimeException("Argument should be a table " + spec);
    }

    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();
    Table tableOnSrc = srcMs.getTable(spec.getDbName(), spec.getTableName());

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();

    // TODO: 2019/10/8
    // change src db to dest db , according configed rule
    Table tableOnDest = destMs.getTable(this.destObjectFactory.modifyDestDb(spec.getDbName()),
            spec.getTableName());

    // If the souce table doesn't exist but the destination table doesn't,
    // then it's most likely a drop.
    if (tableOnSrc == null && tableOnDest != null) {

      if(conf.getBoolean(ConfigurationKeys.BATCH_JOB_DROP_TABLE_BOOLEAN, true)){
        return new TaskEstimate(TaskEstimate.TaskType.DROP_TABLE, false, false, Optional.empty(),
                Optional.empty());
      }else {
        return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
                Optional.empty());
      }
    }

    // Nothing to do if the source table doesn't exist
    if (tableOnSrc == null) {
      return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
          Optional.empty());
    }

    // If both src and dest exist, and the dest is newer, and we don't overwrite newer partitions,
    // then it's a NO_OP.
    // we use true ,will overwrite dest tabel , src is always the right table .
    if (!conf.getBoolean(ConfigurationKeys.BATCH_JOB_OVERWRITE_NEWER, true)) {
      if (ReplicationUtils.isSrcOlder(tableOnSrc, tableOnDest)) {
        LOG.warn(String.format(
            "Source %s (%s) is older than destination (%s), so not copying",
            spec,
            ReplicationUtils.getLastModifiedTime(tableOnSrc),
            ReplicationUtils.getLastModifiedTime(tableOnDest)));
        return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
            Optional.empty());
      }
    }

    boolean isPartitionedTable = HiveUtils.isPartitioned(tableOnSrc);

    // See if we need to update the data
    // Locations are not defined for views
    boolean updateData = false;
    Optional<Path> srcPath = ReplicationUtils.getLocation(tableOnSrc);

    Table expectedDestTable =
        destObjectFactory.createDestTable(srcCluster, destCluster, tableOnSrc, tableOnDest);

    Optional<Path> destPath = ReplicationUtils.getLocation(expectedDestTable);

    if (!isPartitionedTable && srcPath.isPresent() && !srcPath.equals(destPath)) {
      updateData = !directoryCopier.equalDirs(srcPath.get(), destPath.get(), true);
    }

    // See if we need to update the metadata
    boolean updateMetadata =
        tableOnDest == null || !ReplicationUtils.stripNonComparables(tableOnDest)
            .equals(ReplicationUtils.stripNonComparables(expectedDestTable));

    if (!updateData && !updateMetadata) {
      return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
          Optional.empty());
    } else if (!isPartitionedTable) {

      if(conf.getBoolean(ConfigurationKeys.BATCH_JOB_PARTITION_TABLE_ONLY_BOOLEAN, false)){
        return new TaskEstimate(TaskEstimate.TaskType.NO_OP, updateMetadata,
                updateData, srcPath, destPath);
      }else {
        return new TaskEstimate(TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE, updateMetadata,
                updateData, srcPath, destPath);
      }

    } else {
      return new TaskEstimate(TaskEstimate.TaskType.COPY_PARTITIONED_TABLE, true, false,
          Optional.empty(), Optional.empty());
    }
  }

  private TaskEstimate analyzePartitionSpec(HiveObjectSpec spec)
      throws HiveMetastoreException, IOException {

    if (!spec.isPartition()) {
      throw new RuntimeException("Argument should be a partition " + spec);
    }

    boolean updateData = false;

    // if partition appointed ,return NO_OP
    if(conf.getBoolean(ConfigurationKeys.BATCH_JOB_PARTITION_APPOINT,  false)){
      if(StringUtils.isNotBlank(conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_START))){
        if(spec.getPartitionName().length() ==
                conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_START).length() ){
          // partiton schedeme same
          if(
                  spec.getPartitionName().compareTo(
                          conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_START)) < 0 ){
            LOG.info(String.format(
                    "appoint partition %s and %s ,but table %s not between ,return TaskType.NO_OP",
                    conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_START),
                    conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_END),
                    spec
                    )

            );
            return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
                    Optional.empty());
          }

        }else {
          // partition scheme isn't same ,do nothing

        }
      }

      if(StringUtils.isNotBlank(conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_END))){
        if(spec.getPartitionName().length() ==
                conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_END).length() ){
          // partiton schedeme same
          if( spec.getPartitionName().compareTo(
                          conf.get(ConfigurationKeys.BATCH_JOB_PARTITION_END )) > 0 ){

            LOG.info(String.format(
                    "appoint partition %s and %s ,but table %s not between ,return TaskType.NO_OP",
                    conf.getTrimmed(ConfigurationKeys.BATCH_JOB_PARTITION_START),
                    conf.getTrimmed(ConfigurationKeys.BATCH_JOB_PARTITION_END),
                    spec
                    )

            );
            return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
                    Optional.empty());
          }
        }else {
          // partition scheme isn't same ,do nothing

        }
      }

    }

    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();
    Partition partitionOnSrc =
        srcMs.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();

    // change dest to new db
    Partition partitionOnDest = destMs.getPartition(
            this.destObjectFactory.modifyDestDb(spec.getDbName()),
            spec.getTableName(), spec.getPartitionName());

    // If the source partition does not exist, but the destination does,
    // it's most likely a drop.
    if (partitionOnSrc == null && partitionOnDest != null) {
      if(conf.getBoolean(ConfigurationKeys.BATCH_JOB_DROP_PARTITION_BOOLEAN, true )){
        return new TaskEstimate(TaskEstimate.TaskType.DROP_PARTITION, false, false, Optional.empty(),
                Optional.empty());
      }else {

        LOG.info("partitionOnSrc null ,partitionOnDest exist, but config BATCH_JOB_DROP_PARTITION_BOOLEAN is false ,return TaskType.NO_OP ," +
                partitionOnDest
                );
        return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
                Optional.empty());
      }


    }

    if (partitionOnSrc == null) {
      return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
          Optional.empty());
    }

    // If both src and dest exist, and the dest is newer, and we don't overwrite newer partitions,
    // then it's a NO_OP.
    if (!conf.getBoolean(ConfigurationKeys.BATCH_JOB_OVERWRITE_NEWER, true)) {
      if (ReplicationUtils.isSrcOlder(partitionOnSrc, partitionOnDest)) {
        LOG.warn(String.format(
            "Source %s (%s) is older than destination (%s), so not copying",
            spec,
            ReplicationUtils.getLastModifiedTime(partitionOnSrc),
            ReplicationUtils.getLastModifiedTime(partitionOnDest)));
        return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
            Optional.empty());
      }
    }

    // change dest to new db
    Partition expectedDestPartition = destObjectFactory.createDestPartition(srcCluster, destCluster,
        partitionOnSrc, partitionOnDest);

    Optional<Path> srcPath = ReplicationUtils.getLocation(partitionOnSrc);
    Optional<Path> destPath = ReplicationUtils.getLocation(expectedDestPartition);

    // See if we need to update the data
    // TODO: 2019/12/7 增加逻辑，由于之前同步分区下有子目录存在问题，该处仅处理分区下面有子文件夹的分区

    if ( conf.getBoolean(ConfigurationKeys.BATCH_JOB_SUBDIR_ONLY, false) ){


    }


    if (srcPath.isPresent()
            // TODO: 2019/12/7  是否需要同步数据，此处有bug？该判断逻辑应该去掉，直接判断文件夹是否相等？　
            && !srcPath.equals(destPath)
    ) {
      updateData = !directoryCopier.equalDirs(srcPath.get(), destPath.get(), true);
    }

    // A metadata update is required if the destination partition doesn't
    // exist or the metadata differs from what's expected.
    boolean updateMetadata =
        partitionOnDest == null || !ReplicationUtils.stripNonComparables(partitionOnDest)
            .equals(ReplicationUtils.stripNonComparables(expectedDestPartition));

    if (!updateData && !updateMetadata) {
      return new TaskEstimate(TaskEstimate.TaskType.NO_OP, false, false, Optional.empty(),
          Optional.empty());
    } else {
      return new TaskEstimate(TaskEstimate.TaskType.COPY_PARTITION, updateMetadata, updateData,
          srcPath, destPath);
    }
  }
}
