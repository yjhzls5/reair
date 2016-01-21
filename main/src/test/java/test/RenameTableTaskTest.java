package test;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.primitives.CopyUnpartitionedTableTask;
import com.airbnb.di.hive.replication.primitives.RenameTableTask;
import com.airbnb.di.multiprocessing.ParallelJobExecutor;
import com.airbnb.di.utils.ReplicationTestUtils;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RenameTableTaskTest extends MockClusterTest {

  private static ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);

  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    jobExecutor.start();
  }

  @Test
  public void testRenameTable() throws IOException, HiveMetastoreException, DistCpException {
    String dbName = "test_db";
    String tableName = "test_table";
    String newTableName = "new_test_table";

    // Create an unpartitioned table in the source
    HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Copy the table
    CopyUnpartitionedTableTask copyJob = new CopyUnpartitionedTableTask(conf,
        destinationObjectFactory, conflictHandler, srcCluster, destCluster, originalTableSpec,
        ReplicationUtils.getLocation(srcTable), directoryCopier, true);
    RunInfo status = copyJob.runTask();

    // Rename the source table
    Table originalSrcTable = new Table(srcTable);
    srcTable.setTableName(newTableName);
    srcMetastore.alterTable(dbName, tableName, srcTable);
    HiveObjectSpec newTableSpec = new HiveObjectSpec(dbName, newTableName);
    ReplicationTestUtils.updateModifiedTime(srcMetastore, newTableSpec);

    // Propagate the rename
    RenameTableTask job = new RenameTableTask(conf, srcCluster, destCluster,
        destinationObjectFactory, conflictHandler, originalTableSpec, newTableSpec,
        ReplicationUtils.getLocation(originalSrcTable), ReplicationUtils.getLocation(srcTable),
        ReplicationUtils.getTldt(originalSrcTable), jobExecutor, directoryCopier);

    RunInfo runInfo = job.runTask();

    // Check to make sure that the rename has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsTable(newTableSpec.getDbName(), newTableSpec.getTableName()));
    assertFalse(
        destMetastore.existsTable(originalTableSpec.getDbName(), originalTableSpec.getTableName()));
    assertEquals(ReplicationTestUtils.getModifiedTime(srcMetastore, newTableSpec),
        ReplicationTestUtils.getModifiedTime(destMetastore, newTableSpec));
  }

  @Test
  public void testRenameTableReqiringCopy()
      throws IOException, HiveMetastoreException, DistCpException {
    String dbName = "test_db";
    String tableName = "test_table";
    String newTableName = "new_test_table";

    // Create an unpartitioned table in the source
    HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);


    // Rename the source table. Note that the source table wasn't copied to
    // the destination.
    Table originalSrcTable = new Table(srcTable);
    srcTable.setTableName(newTableName);
    srcMetastore.alterTable(dbName, tableName, srcTable);
    HiveObjectSpec newTableSpec = new HiveObjectSpec(dbName, newTableName);
    ReplicationTestUtils.updateModifiedTime(srcMetastore, newTableSpec);

    // Propagate the rename
    RenameTableTask job = new RenameTableTask(conf, srcCluster, destCluster,
        destinationObjectFactory, conflictHandler, originalTableSpec, newTableSpec,
        ReplicationUtils.getLocation(originalSrcTable), ReplicationUtils.getLocation(srcTable),
        ReplicationUtils.getTldt(originalSrcTable), jobExecutor, directoryCopier);

    RunInfo runInfo = job.runTask();

    // Check to make sure that the expected table exists has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsTable(newTableSpec.getDbName(), newTableSpec.getTableName()));
    assertFalse(
        destMetastore.existsTable(originalTableSpec.getDbName(), originalTableSpec.getTableName()));
  }
}
