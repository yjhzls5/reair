package test;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.primitives.CopyPartitionTask;
import com.airbnb.di.hive.replication.primitives.CopyUnpartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.CopyPartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;
import com.airbnb.di.hive.replication.primitives.TaskEstimator;
import com.airbnb.di.utils.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskEstimatorTest extends MockClusterTest {
  private static final Log LOG = LogFactory.getLog(TaskEstimatorTest.class);

  // Common names to use for testing
  private static final String HIVE_DB = "test_db";
  private static final String HIVE_TABLE = "test_table";
  private static final String HIVE_PARTITION = "ds=1/hr=1";

  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
  }

  @Test
  public void testEstimatesForUnpartitionedTable()
      throws IOException, HiveMetastoreException, DistCpException {

    DirectoryCopier directoryCopier = new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an unpartitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);

    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);

    // Table exists in source, but not in dest. It should copy the table.
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcTable.getSd().getLocation())));

    // Replicate the table
    CopyUnpartitionedTableTask copyJob =
        new CopyUnpartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, spec, ReplicationUtils.getLocation(srcTable), directoryCopier, true);
    RunInfo status = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());

    // A copy has been made on the destination. Now it shouldn't need to do
    // anything.
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Change the the source metadata. It should now require a metadata
    // update.
    srcTable.getParameters().put("foo", "bar");
    srcMetastore.alterTable(HIVE_DB, HIVE_TABLE, srcTable);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Change the source data. It should now require a data update as well.
    ReplicationTestUtils.createTextFile(conf, new Path(srcTable.getSd().getLocation()),
        "new_file.txt", "456");
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcTable.getSd().getLocation())));

    // Drop the source. It should now be a drop.
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.DROP_TABLE);
  }

  @Test
  public void testEstimatesForPartitionedTable()
      throws IOException, HiveMetastoreException, DistCpException {

    DirectoryCopier directoryCopier = new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an partitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);

    Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);

    // Table exists in source, but not in dest. It should copy the table.
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Replicate the table
    CopyPartitionedTableTask copyJob = new CopyPartitionedTableTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, spec, ReplicationUtils.getLocation(srcTable));
    RunInfo status = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());

    // A copy has been made on the destination. Now it shouldn't need to do
    // anything.
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Change the the source metadata. It should now require a metadata
    // update.
    srcTable.getParameters().put("foo", "bar");
    srcMetastore.alterTable(HIVE_DB, HIVE_TABLE, srcTable);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Drop the source. It should now be a drop.
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.DROP_TABLE);
  }

  @Test
  public void testEstimatesForPartition()
      throws IOException, HiveMetastoreException, DistCpException {

    DirectoryCopier directoryCopier = new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);
    Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create a partition in the source
    HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE, HIVE_PARTITION);
    Partition srcPartition = ReplicationTestUtils.createPartition(conf, srcMetastore, spec);

    TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);

    // Partition exists in source, but not in dest. It should copy the
    // partition.
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcPartition.getSd().getLocation())));

    // Replicate the partition
    CopyPartitionTask copyJob = new CopyPartitionTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, spec, ReplicationUtils.getLocation(srcTable),
        Optional.<Path>empty(), directoryCopier, true);
    RunInfo status = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());

    // A copy has been made on the destination. Now it shouldn't need to do
    // anything.
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Change the the source metadata. It should now require a metadata
    // update.
    srcPartition.getParameters().put("foo", "bar");
    srcMetastore.alterPartition(HIVE_DB, HIVE_TABLE, srcPartition);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Change the source data. It should now require a data update as well.
    ReplicationTestUtils.createTextFile(conf, new Path(srcPartition.getSd().getLocation()),
        "new_file.txt", "456");
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcPartition.getSd().getLocation())));

    // Drop the source. It should now be a drop.
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.DROP_PARTITION);
  }
}
