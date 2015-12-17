package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveParameterKeys;
import com.airbnb.di.hive.common.HiveMetastoreException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;


public class ObjectConflictHandler {

    private static final Log LOG = LogFactory.getLog(
            ObjectConflictHandler.class);

    public boolean handleCopyConflict(Cluster srcCluster,
                                      Cluster destCluster,
                                      Table srcTable,
                                      Table existingDestTable)
            throws HiveMetastoreException {
        HiveObjectSpec spec = new HiveObjectSpec(existingDestTable.getDbName(),
                existingDestTable.getTableName());

        if (existingDestTable.getParameters()
                .get(HiveParameterKeys.SRC_CLUSTER) != srcCluster.getName()) {
            // TODO: If there is a conflict, rename the table appropriately
            LOG.warn("Table " + spec + " exists on destination, and it's " +
                    "missing tags that indicate that it was replicated.");
        }

        // If the partitioning keys don't match, then it will have to be
        // dropped.
        if (!srcTable.getPartitionKeys().equals(
                existingDestTable.getPartitionKeys())) {
            // Table exists on destination, but it's partitioned. It'll have to
            // be dropped.
            // TODO: If there is a conflict, rename instead of dropping
            LOG.warn(String.format("For %s, there is a mismatch in the " +
                    "partitioning keys. src: %s dest: %s",
                    spec,
                    srcTable.getPartitionKeys(),
                    existingDestTable.getPartitionKeys()));

            boolean dropData = true;
            // TODO: Make safeguard for s3a and s3n more elegant
            if (existingDestTable.getSd() != null &&
                    existingDestTable.getSd().getLocation() != null) {
                String location = existingDestTable.getSd().getLocation();
                if (location.startsWith("s3n") || location.startsWith("s3a")) {
                    LOG.warn("Not removing data for " + spec + " since it " +
                            "points to s3: " + location);
                    dropData = false;
                }
            }
            HiveMetastoreClient destMs = destCluster.getMetastoreClient();

            LOG.info(String.format("Dropping %s on destination (delete " +
                            "data: %s)",
                    spec, dropData));
            destMs.dropTable(spec.getDbName(), spec.getTableName(), dropData);
            LOG.info("Dropped " + spec);
        }

        return true;
    }

    public boolean handleCopyConflict(Cluster srcCluster,
                                      Cluster destCluster,
                                      Partition srcPartition,
                                      Partition existingDestPartition) {
        // Partitions can be usually overwritten without issues
        return true;
    }
}
