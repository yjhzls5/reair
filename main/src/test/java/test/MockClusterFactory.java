package test;

import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.ClusterFactory;

import com.airbnb.di.hive.replication.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;

/**
 * Returns static instances of Clusters for testing.
 */
public class MockClusterFactory implements ClusterFactory {
  @Override
  public void setConf(Configuration conf) {
  }

  @Override
  public Cluster getSrcCluster() {
    return MockClusterTest.srcCluster;
  }

  @Override
  public Cluster getDestCluster() {
    return MockClusterTest.destCluster;
  }

  @Override
  public DirectoryCopier getDirectoryCopier() throws ConfigurationException {
    return MockClusterTest.directoryCopier;
  }
}
