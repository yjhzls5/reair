package test;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.configuration.Cluster;
import org.apache.hadoop.fs.Path;

/**
 * Created by paul_yang on 7/15/15.
 */
public class MockCluster implements Cluster {

    private String name;
    private HiveMetastoreClient client;
    private Path fsRoot;
    private Path tmpDir;

    public MockCluster(String name, HiveMetastoreClient client, Path fsRoot,
                       Path tmpDir) {
        this.name = name;
        this.client = client;
        this.fsRoot = fsRoot;
        this.tmpDir = tmpDir;
    }

    @Override
    public HiveMetastoreClient getMetastoreClient() throws HiveMetastoreException {
        return client;
    }

    @Override
    public Path getFsRoot() {
        return fsRoot;
    }

    @Override
    public Path getTmpDir() {
        return tmpDir;
    }

    @Override
    public String getName() {
        return name;
    }
}
