package test;

import com.airbnb.di.common.ConfigurationKeys;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.DirectoryCopier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by paul_yang on 7/15/15.
 */
public abstract class MockClusterTest {

    private static final Log LOG = LogFactory.getLog(
            MockClusterTest.class);

    protected static MockHiveMetastoreClient srcMetastore;
    protected static MockHiveMetastoreClient destMetastore;

    protected static YarnConfiguration conf;
    protected static MiniYARNCluster miniCluster;

    protected static Cluster srcCluster;
    protected static Cluster destCluster;

    protected static DirectoryCopier directoryCopier;

    // Temporary directories on the local filesystem that we'll treat as the
    // source and destination filesystems
    @Rule
    public TemporaryFolder srcLocalTmp = new TemporaryFolder();
    @Rule
    public TemporaryFolder destLocalTmp = new TemporaryFolder();

    protected Path srcWarehouseRoot;
    protected Path destWarehouseRoot;

    @BeforeClass
    public static void setupClass() throws IOException, SQLException {
        conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        conf.set(ConfigurationKeys.DISTCP_POOL, "default_pool");
        miniCluster = new MiniYARNCluster(
                "test", 1, 1, 1);
        miniCluster.init(conf);
             miniCluster.start();

    }

    @Before
    public void setUp() throws IOException {
        srcMetastore = new MockHiveMetastoreClient();
        destMetastore = new MockHiveMetastoreClient();

        srcLocalTmp.create();
        destLocalTmp.create();

        Path srcFsRoot = new Path("file://" +
                srcLocalTmp.getRoot().getAbsolutePath());
        Path destFsRoot = new Path("file://" +
                destLocalTmp.getRoot().getAbsolutePath());

        srcWarehouseRoot =  new Path(makeFileURI(srcLocalTmp),
                "warehouse");
        destWarehouseRoot = new Path(makeFileURI(destLocalTmp),
                "warehouse");

        System.out.println(String.format("src root: %s, dest root: %s",
                srcWarehouseRoot, destWarehouseRoot));

        Path srcTmp = new Path(makeFileURI(this.srcLocalTmp), "tmp");
        Path destTmp = new Path(makeFileURI(this.destLocalTmp), "tmp");

        srcCluster = new MockCluster("src_cluster", srcMetastore,
                srcFsRoot, srcTmp);
        destCluster = new MockCluster("dest_cluster", destMetastore,
                destFsRoot, destTmp);

        // Disable checking of modified times as the local filesystem does not
        // support this
        directoryCopier = new DirectoryCopier(
                conf,
                destCluster.getTmpDir(),
                false);
    }

    @AfterClass
    public static void tearDownClass() {
        miniCluster.stop();
    }

    protected static Path makeFileURI(TemporaryFolder directory) {
        return new Path("file://" + directory.getRoot().getAbsolutePath());
    }
}
