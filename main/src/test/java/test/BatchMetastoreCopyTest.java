package test;

import com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

/**
 * Unit Test for MetastoreReplicationJob
 */
public class BatchMetastoreCopyTest extends MockClusterTest {
    @BeforeClass
    public static void setupClass() throws IOException, SQLException {
        MockClusterTest.setupClass();
   }


    @Test
    public void testCopyNewTables() throws Exception {
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

        String[] args = {};
        jobConf.set("airbnb.reair.clusters.batch.output.dir",
                new Path(destCluster.getFsRoot(), "test_output").toString());
        jobConf.set("airbnb.reair.clusters.batch.test.injection.class", "test.MockClusterTest");

        ToolRunner.run(jobConf, new MetastoreReplicationJob(), args);

        assertTrue(ReplicationUtils.exists(destMetastore, spec));

        assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec1));
        assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec2));
        assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec3));

        ReplicationTestUtils.dropTable(srcMetastore, spec);
        ReplicationTestUtils.dropPartition(srcMetastore, partitionSpec2);

        ToolRunner.run(jobConf, new MetastoreReplicationJob(), args);

        assertTrue(!ReplicationUtils.exists(destMetastore, partitionSpec2));
    }
}
