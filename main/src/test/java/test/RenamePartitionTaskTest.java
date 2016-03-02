package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.primitives.CopyPartitionTask;
import com.airbnb.di.hive.replication.primitives.RenamePartitionTask;
import com.airbnb.di.multiprocessing.ParallelJobExecutor;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RenamePartitionTaskTest extends MockClusterTest {
  private static ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);

  /**
   * TODO.
   *
   * @throws IOException TODO
   * @throws SQLException TODO
   */
  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    jobExecutor.start();
  }

  @Test
  public void testRenamePartition() throws IOException, HiveMetastoreException, DistCpException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String oldPartitionName = "ds=1/hr=1";
    final String newPartitionName = "ds=1/hr=2";

    // Create an partitioned table in the source
    final HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    final HiveObjectSpec oldPartitionSpec = new HiveObjectSpec(dbName, tableName, oldPartitionName);
    final HiveObjectSpec newPartitionSpec = new HiveObjectSpec(dbName, tableName, newPartitionName);

    final Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);

    final Partition oldPartition =
        ReplicationTestUtils.createPartition(conf, srcMetastore, oldPartitionSpec);

    // Copy the partition
    final Configuration testConf = new Configuration(conf);
    final CopyPartitionTask copyJob = new CopyPartitionTask(testConf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, oldPartitionSpec,
        ReplicationUtils.getLocation(oldPartition), Optional.empty(), directoryCopier, true);

    final RunInfo status = copyJob.runTask();

    // Rename the source partition
    final Partition newPartition = new Partition(oldPartition);
    final List<String> newValues = new ArrayList<>();
    newValues.add("1");
    newValues.add("2");
    newPartition.setValues(newValues);

    srcMetastore.renamePartition(dbName, tableName, oldPartition.getValues(), newPartition);

    // Propagate the rename
    final RenamePartitionTask task = new RenamePartitionTask(testConf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, oldPartitionSpec, newPartitionSpec,
        ReplicationUtils.getLocation(oldPartition), ReplicationUtils.getLocation(newPartition),
        ReplicationUtils.getTldt(oldPartition), directoryCopier);

    final RunInfo runInfo = task.runTask();

    // Check to make sure that the rename has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsPartition(newPartitionSpec.getDbName(),
        newPartitionSpec.getTableName(), newPartitionSpec.getPartitionName()));
    assertFalse(destMetastore.existsPartition(oldPartitionSpec.getDbName(),
        oldPartitionSpec.getTableName(), oldPartitionSpec.getPartitionName()));
    assertEquals(ReplicationTestUtils.getModifiedTime(srcMetastore, newPartitionSpec),
        ReplicationTestUtils.getModifiedTime(destMetastore, newPartitionSpec));
  }
}
