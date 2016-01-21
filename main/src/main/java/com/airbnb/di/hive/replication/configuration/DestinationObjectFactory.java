package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.common.FsUtils;
import com.airbnb.di.hive.common.HiveParameterKeys;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.configuration.Cluster;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Creates the Hive thrift object that should be created on the destination cluster. Note that only
 * the Thrift object is generated - it's not actually created in the metastore.
 */
public class DestinationObjectFactory implements Configurable {

  private Optional<Configuration> conf;

  public DestinationObjectFactory() {}

  public void setConf(Configuration conf) {
    this.conf = Optional.ofNullable(conf);
  }

  public Configuration getConf() {
    return conf.orElse(null);
  }

  /**
   * For objects with a location, transform the location through this method
   * 
   * @param srcCluster
   * @param destCluster
   * @param srcLocation
   * @return
   */
  public String modifyLocation(Cluster srcCluster, Cluster destCluster, String srcLocation) {
    Path srcPath = new Path(srcLocation);

    String scheme = srcPath.toUri().getScheme();
    if (scheme != null) {
      // Objects with an s3 location should be copied as is
      if (scheme.equals("s3n") || scheme.equals("s3a")) {
        return srcLocation;
      }
    }

    // The destination path should have the same relative path under the
    // destination FS's root.

    // If the source path is within the FS root of the source cluster,
    // it should have the same relative path on the destination
    Path destPath;
    if (srcPath.toString().startsWith(srcCluster.getFsRoot().toString() + "/")) {
      String relativePath = FsUtils.getRelativePath(srcCluster.getFsRoot(), srcPath);
      destPath = new Path(destCluster.getFsRoot(), relativePath);
    } else {
      destPath = new Path(destCluster.getFsRoot(), srcPath.toUri().getPath());
    }

    return destPath.toString();
  }

  /**
   *
   * @param srcCluster
   * @param destCluster
   * @param srcTable Table object from the source
   * @param existingDestTable Table object from the destination, if one already exists
   * @return the table to create or overwrite with on the destination.
   */
  public Table createDestTable(
      Cluster srcCluster,
      Cluster destCluster,
      Table srcTable,
      Table existingDestTable) {
    Table destTable = new Table(srcTable);

    // If applicable, update the location for the table
    Optional<Path> srcLocation = ReplicationUtils.getLocation(srcTable);
    if (srcLocation.isPresent() && !srcLocation.get().toString().startsWith("s3")) {
      String destLocation = modifyLocation(srcCluster, destCluster, srcLocation.get().toString());

      destTable.getSd().setLocation(destLocation);
    }
    destTable.putToParameters(HiveParameterKeys.SRC_CLUSTER, srcCluster.getName());

    // Merge the parameters for the table, with the parameter values from
    // the source taking precedence
    if (existingDestTable != null) {
      Map<String, String> newParameters = new HashMap<>();
      newParameters.putAll(existingDestTable.getParameters());
      newParameters.putAll(destTable.getParameters());
      destTable.setParameters(newParameters);
    }

    return destTable;
  }

  /**
   *
   * @param srcCluster
   * @param destCluster
   * @param srcPartition Partition object from the source
   * @param existingDestPartition Partition object on the destination, if one already exists
   * @return Partition object to create or overwrite with on the destination
   */
  public Partition createDestPartition(
      Cluster srcCluster,
      Cluster destCluster,
      Partition srcPartition,
      Partition existingDestPartition) {
    Partition destPartition = new Partition(srcPartition);

    Optional<Path> srcLocation = ReplicationUtils.getLocation(srcPartition);
    // If applicable, update the location for the partition
    if (srcLocation.isPresent() && !srcLocation.get().toString().startsWith("s3")) {
      String destLocation = modifyLocation(srcCluster, destCluster, srcLocation.get().toString());
      destPartition.getSd().setLocation(destLocation);
    }
    destPartition.putToParameters(HiveParameterKeys.SRC_CLUSTER, srcCluster.getName());

    // Merge the parameters for the partition, with the parameter values
    // from the source taking precedence
    if (existingDestPartition != null) {
      Map<String, String> newParameters = new HashMap<>();
      newParameters.putAll(existingDestPartition.getParameters());
      newParameters.putAll(destPartition.getParameters());
    }

    return destPartition;
  }

  public boolean shouldCopyData(String srcLocation) {
    if (srcLocation.startsWith("s3n://") || srcLocation.startsWith("s3a://")) {
      return false;
    } else {
      return true;
    }
  }
}
