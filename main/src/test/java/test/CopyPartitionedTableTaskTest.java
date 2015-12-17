package test;

import com.airbnb.di.common.ConfigurationKeys;
import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.primitives.CopyPartitionedTableTask;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CopyPartitionedTableTaskTest extends MockClusterTest {
    private static final Log LOG = LogFactory.getLog(
            CopyPartitionedTableTaskTest.class);

    @Test
    public void testCopyPartitionedTable() throws IOException,
            HiveMetastoreException, DistCpException {

        // Create a partitioned table in the source
        HiveObjectSpec spec = new HiveObjectSpec("test_db", "test_table");
        Table srcTable = ReplicationTestUtils.createPartitionedTable(conf,
                srcMetastore,
                spec,
                TableType.MANAGED_TABLE,
                srcWarehouseRoot);

        // Copy the table
        Configuration testConf = new Configuration(conf);
        testConf.set(ConfigurationKeys.DISTCP_POOL, "default_pool");
        CopyPartitionedTableTask copyJob = new CopyPartitionedTableTask(
                testConf,
                new DestinationObjectFactory(),
                new ObjectConflictHandler(),
                srcCluster,
                destCluster,
                spec,
                ReplicationUtils.getLocation(srcTable));
        RunInfo status = copyJob.runTask();

        // Verify that the table exists on the destination, the location is
        // within the destination filesystem, and no data was copied.
        assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
        Table destTable = destMetastore.getTable(spec.getDbName(),
                spec.getTableName());
        assertNotNull(destTable);
        assertTrue(destTable.getSd().getLocation().startsWith(
                destCluster.getFsRoot() + "/"));
        assertEquals(0, status.getBytesCopied());

        // Verify that doing a copy again is a no-op
        RunInfo rerunStatus = copyJob.runTask();
        assertEquals(RunInfo.RunStatus.SUCCESSFUL,
                rerunStatus.getRunStatus());
        assertEquals(0, rerunStatus.getBytesCopied());
    }

    // Additional test cases - copying of other table types such as views?
}
