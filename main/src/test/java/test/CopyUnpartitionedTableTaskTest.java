package test;

import com.airbnb.di.common.ConfigurationKeys;
import com.airbnb.di.common.FsUtils;
import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.primitives.CopyUnpartitionedTableTask;
import com.airbnb.di.hive.replication.configuration.DestinationObjectFactory;
import com.airbnb.di.hive.replication.configuration.ObjectConflictHandler;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CopyUnpartitionedTableTaskTest extends MockClusterTest {
    private static final Log LOG = LogFactory.getLog(
            CopyUnpartitionedTableTaskTest.class);
    /*
    @Test
    public void testExample() throws Exception {
        LOG.info("This is a test");
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                 FifoScheduler.class, ResourceScheduler.class);
        MiniYARNCluster miniCluster = new MiniYARNCluster(
                "test", 1, 1, 1);
        miniCluster.init(conf);
        miniCluster.start();

        conf.set(ConfigurationKeys.FILESYSTEM_CONSISTENCY_WAIT_TIME, "0");
        conf.set(ConfigurationKeys.DISTCP_POOL, "default");

        Path srcPath = new Path("file:///tmp/deleteme");
        Path destPath = new Path("file:///tmp/deleteme_copy");
        Path distcpTmpDir = new Path("file:///tmp/deleteme_distcp_tmp");
        Path distcpLogDir = new Path("file:///tmp/deleteme_distcp_logdir");

        LOG.info("Running sync");
        FsUtils.oneWaySync(conf, srcPath, destPath, false, true,
                distcpTmpDir, distcpLogDir, null, 0);

        miniCluster.stop();

        assertTrue(true);
    }
    */

    @Test
    public void testCopyUnpartitionedTable() throws IOException,
            HiveMetastoreException, DistCpException {

        // Create an unpartitioned table in the source
        HiveObjectSpec spec = new HiveObjectSpec("test_db", "test_table");
        Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
                srcMetastore,
                spec,
                TableType.MANAGED_TABLE,
                srcWarehouseRoot);

        // Copy the table
        Configuration testConf = new Configuration(conf);
        testConf.set(ConfigurationKeys.DISTCP_POOL, "default_pool");
        CopyUnpartitionedTableTask copyJob = new CopyUnpartitionedTableTask(
                testConf,
                new DestinationObjectFactory(),
                new ObjectConflictHandler(),
                srcCluster,
                destCluster,
                spec,
                ReplicationUtils.getLocation(srcTable),
                directoryCopier);
        RunInfo status = copyJob.runTask();

        // Verify that the table exists on the destination, the location is
        // within the destination filesystem, the data is the same,
        // and the right number of bytes were copied.
        assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
        Table destTable = destMetastore.getTable(spec.getDbName(),
                spec.getTableName());
        assertNotNull(destTable);
        assertTrue(destTable.getSd().getLocation().startsWith(
                destCluster.getFsRoot() + "/"));
        assertTrue(FsUtils.equalDirs(conf,
                new Path(srcTable.getSd().getLocation()),
                new Path(destTable.getSd().getLocation())));
        assertEquals(9, status.getBytesCopied());

        // Verify that doing a copy again is a no-op
        RunInfo rerunStatus = copyJob.runTask();
        assertEquals(RunInfo.RunStatus.SUCCESSFUL,
                rerunStatus.getRunStatus());
        assertEquals(0, rerunStatus.getBytesCopied());

    }

    @Test
    public void testCopyUnpartitionedTableView()
            throws IOException, HiveMetastoreException, DistCpException {
        HiveObjectSpec spec = new HiveObjectSpec("test_db", "test_table_view");
        Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
                srcMetastore,
                spec,
                TableType.VIRTUAL_VIEW,
                srcWarehouseRoot);

        // Copy the table
        Configuration testConf = new Configuration(conf);
        testConf.set(ConfigurationKeys.DISTCP_POOL, "default_pool");
        CopyUnpartitionedTableTask copyJob = new CopyUnpartitionedTableTask(
                testConf,
                new DestinationObjectFactory(),
                new ObjectConflictHandler(),
                srcCluster,
                destCluster,
                spec,
                ReplicationUtils.getLocation(srcTable),
                directoryCopier);
        RunInfo status = copyJob.runTask();

        // Verify that the table exists on the destination, the location is
        // within the destination filesystem, the data is the same,
        // and the right number of bytes were copied.
        assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
        Table destTable = destMetastore.getTable(spec.getDbName(),
                spec.getTableName());
        assertNotNull(destTable);
        assertNull(destTable.getSd().getLocation());
        assertEquals(0, status.getBytesCopied());
    }
}