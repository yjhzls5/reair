package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveParameterKeys;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.ReplicationUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Optional;

/**
 * Handles cases when there is an existing table or partition on the destination cluster. This class
 * can later be configured to be user-defined.
 */
public class ObjectConflictHandler implements Configurable {

  private static final Log LOG = LogFactory.getLog(ObjectConflictHandler.class);

  private Optional<Configuration> conf;

  public void setConf(Configuration conf) {
    this.conf = Optional.ofNullable(conf);
  }

  public Configuration getConf() {
    return conf.orElse(null);
  }

  public boolean handleCopyConflict(
      Cluster srcCluster,
      Cluster destCluster,
      Table srcTable,
      Table existingDestTable) throws HiveMetastoreException {
    HiveObjectSpec spec =
        new HiveObjectSpec(existingDestTable.getDbName(), existingDestTable.getTableName());

    if (existingDestTable.getParameters().get(HiveParameterKeys.SRC_CLUSTER) != srcCluster
        .getName()) {
      LOG.warn("Table " + spec + " exists on destination, and it's "
          + "missing tags that indicate that it was replicated.");
      // This might indicate that someone created a table with the same
      // name on the destination cluster. Instead of dropping and
      // overwriting, a rename can be done here to save the table with a
      // *_conflict name.
    }

    // If the partitioning keys don't match, then it will have to be
    // dropped.
    if (!srcTable.getPartitionKeys().equals(existingDestTable.getPartitionKeys())) {
      // Table exists on destination, but it's partitioned. It'll have to
      // be dropped since Hive doesn't support changing of partition
      // columns. Instead of dropping the table, the table on the dest
      // cluster could be renamed to something else for further
      // inspection.
      LOG.warn(String.format(
          "For %s, there is a mismatch in the " + "partitioning keys. src: %s dest: %s", spec,
          srcTable.getPartitionKeys(), existingDestTable.getPartitionKeys()));

      boolean dropData = !locationOnS3(existingDestTable.getSd());
      LOG.warn("Not dropping data at location " + ReplicationUtils.getLocation(existingDestTable));
      HiveMetastoreClient destMs = destCluster.getMetastoreClient();

      LOG.debug(String.format("Dropping %s on destination (delete " + "data: %s)", spec, dropData));
      destMs.dropTable(spec.getDbName(), spec.getTableName(), dropData);
      LOG.debug("Dropped " + spec);
    }

    return true;
  }

  public boolean handleCopyConflict(Cluster srcCluster, Cluster destCluster, Partition srcPartition,
      Partition existingDestPartition) {
    // Partitions can be usually overwritten without issues
    return true;
  }

  private boolean locationOnS3(StorageDescriptor sd) {
    String location = sd.getLocation();

    return location != null && (location.startsWith("s3n") || location.startsWith("s3a"));
  }
}
