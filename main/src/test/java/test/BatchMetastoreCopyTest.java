package test;

import static org.junit.Assert.assertTrue;

import com.airbnb.di.hive.batchreplication.hdfscopy.ReplicationJob;
import com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Unit Test for MetastoreReplicationJob.
 */
public class BatchMetastoreCopyTest extends MockClusterTest {

  /**
   * TODO.
   *
   * @throws IOException TODO
   * @throws SQLException TODO
   */
  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
  }

  @Test
  public void testCopyNewTables() throws Exception {
    // Create an unpartitioned table in the source
    final HiveObjectSpec spec = new HiveObjectSpec("test", "test_table");
    final Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
        srcMetastore,
        spec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create a partitioned table in the source
    final HiveObjectSpec tableSpec = new HiveObjectSpec("test", "partitioned_table");
    final Table srcTable2 = ReplicationTestUtils.createPartitionedTable(conf,
        srcMetastore,
        tableSpec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create several partitions in the source table
    HiveObjectSpec partitionSpec1 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=1");
    HiveObjectSpec partitionSpec2 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=2");
    HiveObjectSpec partitionSpec3 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=3");

    final Partition srcPartition1 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec1);
    final Partition srcPartition2 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec2);
    final Partition srcPartition3 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec3);

    JobConf jobConf = new JobConf(conf);

    String[] args = {};
    jobConf.set(DeployConfigurationKeys.BATCH_JOB_OUTPUT_DIR,
        new Path(destCluster.getFsRoot(), "test_output").toString());
    jobConf.set(DeployConfigurationKeys.BATCH_JOB_CLUSTER_FACTORY_CLASS,
        MockClusterFactory.class.getName());

    ToolRunner.run(jobConf, new MetastoreReplicationJob(), args);

    assertTrue(ReplicationUtils.exists(destMetastore, spec));

    Table dstTable = destMetastore.getTable(spec.getDbName(), spec.getTableName());
    assertTrue(directoryCopier.equalDirs(new Path(srcTable.getSd().getLocation()),
          new Path(dstTable.getSd().getLocation())));

    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec1));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec2));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec3));

    Partition dstPartition1 = destMetastore.getPartition(partitionSpec1.getDbName(),
        partitionSpec1.getTableName(),
        partitionSpec1.getPartitionName());
    assertTrue(directoryCopier.equalDirs(new Path(srcPartition1.getSd().getLocation()),
          new Path(dstPartition1.getSd().getLocation())));

    Partition dstPartition2 = destMetastore.getPartition(partitionSpec2.getDbName(),
        partitionSpec2.getTableName(),
        partitionSpec2.getPartitionName());
    assertTrue(directoryCopier.equalDirs(new Path(srcPartition2.getSd().getLocation()),
          new Path(dstPartition2.getSd().getLocation())));

    Partition dstPartition3 = destMetastore.getPartition(partitionSpec3.getDbName(),
        partitionSpec3.getTableName(),
        partitionSpec3.getPartitionName());
    assertTrue(directoryCopier.equalDirs(new Path(srcPartition3.getSd().getLocation()),
          new Path(dstPartition3.getSd().getLocation())));

    ReplicationTestUtils.dropTable(srcMetastore, spec);
    ReplicationTestUtils.dropPartition(srcMetastore, partitionSpec2);

    ToolRunner.run(jobConf, new MetastoreReplicationJob(), args);

    assertTrue(!ReplicationUtils.exists(destMetastore, partitionSpec2));
  }


  @Test
  public void testHdfsCopy() throws Exception {
    // Create an unpartitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec("test", "test_table");
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
        srcMetastore,
        spec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test", "partitioned_table");
    Table srcTable2 = ReplicationTestUtils.createPartitionedTable(conf,
        srcMetastore,
        tableSpec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create several partitions in the source table
    HiveObjectSpec partitionSpec1 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=1");
    HiveObjectSpec partitionSpec2 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=2");
    HiveObjectSpec partitionSpec3 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=3");

    Partition srcPartition1 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec1);
    Partition srcPartition2 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec2);
    Partition srcPartition3 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec3);

    JobConf jobConf = new JobConf(conf);

    String[] args = {"--source", srcWarehouseRoot.toString(),
        "--destination", destWarehouseRoot.toString(),
        "--output-path", new Path(destCluster.getFsRoot(), "test_output").toString(),
        "--operation", "a,d,u"};

    ToolRunner.run(jobConf, new ReplicationJob(), args);

    assertTrue(directoryCopier.equalDirs(srcWarehouseRoot, destWarehouseRoot));
  }
}
