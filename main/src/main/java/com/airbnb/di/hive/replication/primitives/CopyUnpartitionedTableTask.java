package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveUtils;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.multiprocessing.Lock;
import com.airbnb.di.multiprocessing.LockSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

/**
 * Task that copies an unpartitioned table, copying data if allowed and necessary.
 */
public class CopyUnpartitionedTableTask implements ReplicationTask {
  private static final Log LOG = LogFactory.getLog(CopyUnpartitionedTableTask.class);

  private Configuration conf;
  private DestinationObjectFactory objectModifier;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<Path> tableLocation;
  private DirectoryCopier directoryCopier;
  private boolean allowDataCopy;

  /**
   * TODO.
   *
   * @param conf TODO
   * @param destObjectFactory TODO
   * @param objectConflictHandler TODO
   * @param srcCluster TODO
   * @param destCluster TODO
   * @param spec TODO
   * @param tableLocation TODO
   * @param directoryCopier TODO
   * @param allowDataCopy TODO
   */
  public CopyUnpartitionedTableTask(
      Configuration conf,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<Path> tableLocation, DirectoryCopier directoryCopier,
      boolean allowDataCopy) {
    this.conf = conf;
    this.objectModifier = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.tableLocation = tableLocation;
    this.spec = spec;
    this.directoryCopier = directoryCopier;
    this.allowDataCopy = allowDataCopy;
  }

  /**
   * TODO.
   */
  public RunInfo runTask() throws HiveMetastoreException, DistCpException, IOException {
    LOG.debug("Copying " + spec);

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();
    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

    // Get a fresh copy of the metadata from the source Hive metastore
    Table freshSrcTable = srcMs.getTable(spec.getDbName(), spec.getTableName());

    if (freshSrcTable == null) {
      LOG.warn("Source table " + spec + " doesn't exist, so not " + "copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    if (HiveUtils.isPartitioned(freshSrcTable)) {
      LOG.warn("Source table " + spec + " is a partitioned table, so " + "not copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    // Check the table that exists already in the destination cluster
    Table existingTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    if (existingTable != null) {
      LOG.debug("Table " + spec + " exists on destination");
      objectConflictHandler.handleCopyConflict(srcCluster, destCluster, freshSrcTable,
          existingTable);
    }

    Table destTable =
        objectModifier.createDestTable(srcCluster, destCluster, freshSrcTable, existingTable);

    // Refresh in case the conflict handler did something
    existingTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    // Copy HDFS data if the location has changed in the destination object.
    // Usually, this is the case, but for S3 backed tables, the location
    // doesn't change.

    Optional<Path> srcPath = ReplicationUtils.getLocation(freshSrcTable);
    Optional<Path> destPath = ReplicationUtils.getLocation(destTable);

    boolean needToCopy = srcPath.isPresent() && !srcPath.equals(destPath)
        && !directoryCopier.equalDirs(srcPath.get(), destPath.get());

    long bytesCopied = 0;

    if (needToCopy) {
      if (!allowDataCopy) {
        LOG.debug(String.format("Need to copy %s to %s, but data " + "copy is not allowed", srcPath,
            destPath));
        return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
      }

      // Copy directory
      bytesCopied = directoryCopier.copy(srcPath.get(), destPath.get(),
          Arrays.asList(srcCluster.getName(), spec.getDbName(), spec.getTableName()));
    } else {
      LOG.debug("Not copying data");
    }

    // Figure out what to do with the table
    MetadataAction action = MetadataAction.NOOP;
    if (existingTable == null) {
      action = MetadataAction.CREATE;
    } else if (!ReplicationUtils.stripNonComparables(existingTable)
        .equals(ReplicationUtils.stripNonComparables(destTable))) {
      action = MetadataAction.ALTER;
    }

    switch (action) {

      case CREATE:
        LOG.debug("Creating " + spec + " since it does not exist on " + "the destination");
        ReplicationUtils.createDbIfNecessary(srcMs, destMs, destTable.getDbName());
        LOG.debug("Creating: " + destTable);
        destMs.createTable(destTable);
        LOG.debug("Successfully created " + spec);
        break;

      case ALTER:
        LOG.debug("Altering table " + spec + " on destination");
        LOG.debug("Existing table: " + existingTable);
        LOG.debug("New table: " + destTable);
        destMs.alterTable(destTable.getDbName(), destTable.getTableName(), destTable);
        LOG.debug("Successfully altered " + spec);
        break;

      case NOOP:
        LOG.debug("Destination table is up to date - not doing " + "anything for " + spec);
        break;

      default:
        // TODO throw an exception
    }

    return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, bytesCopied);
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
