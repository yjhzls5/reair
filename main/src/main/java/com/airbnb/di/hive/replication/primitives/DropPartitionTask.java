package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveParameterKeys;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.multiprocessing.Lock;
import com.airbnb.di.multiprocessing.LockSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.Optional;

public class DropPartitionTask implements ReplicationTask {
  private static final Log LOG = LogFactory.getLog(DropPartitionTask.class);

  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<String> srcTldt;

  public DropPartitionTask(
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<String> srcTldt) {
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.srcTldt = srcTldt;
    this.spec = spec;
  }

  @Override
  public RunInfo runTask() throws HiveMetastoreException {
    HiveMetastoreClient ms = destCluster.getMetastoreClient();
    LOG.debug("Looking to drop: " + spec);
    LOG.debug("Source object TLDT is: " + srcTldt);

    if (!srcTldt.isPresent()) {
      LOG.error("For safety, failing drop job since source object " + " TLDT is missing!");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }
    String expectedTldt = srcTldt.get();

    Partition destPartition =
        ms.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());

    if (destPartition == null) {
      LOG.warn("Missing " + spec + " on destination, so can't drop!");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    LOG.debug("Destination object is: " + destPartition);
    String destTldt = destPartition.getParameters().get(HiveParameterKeys.TLDT);

    if (expectedTldt.equals(destTldt)) {
      LOG.debug(String.format("Destination partition %s matches expected" + " TLDT (%s)", spec,
          destTldt));
      LOG.debug("Dropping " + spec);
      ms.dropPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName(), true);
      LOG.debug("Dropped " + spec);
      return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);
    } else {
      LOG.debug(
          String.format("Not dropping %s as source(%s) and " + "destination(%s) TLDT's don't match",
              spec.toString(), srcTldt, destTldt));
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.SHARED, spec.getTableSpec().toString()));
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
