package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.replication.configuration.ClusterFactory;
import com.airbnb.di.hive.replication.configuration.ConfiguredClusterFactory;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Util class for metastore replication.
 */
public class MetastoreReplUtils {
  private MetastoreReplUtils() {
  }

  /**
   * static function to create ClusterFactory object based on configuration. For test environment it
   * will create a mock ClusterFactory.
   *
   * @param conf configuration for the cluster
   * @return ClusterFactory implementation
   *
   * @throws IOException Java reflection exceptions are wrapped in IOException
   */
  public static ClusterFactory createClusterFactory(Configuration conf) throws IOException {
    String clusterFactoryClassName =
        conf.get(DeployConfigurationKeys.BATCH_JOB_CLUSTER_FACTORY_CLASS);
    if (clusterFactoryClassName != null) {
      ClusterFactory factory = null;
      try {
        factory = (ClusterFactory) Class.forName(clusterFactoryClassName).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new IOException(e);
      }
      return factory;
    } else {
      ConfiguredClusterFactory configuredClusterFactory = new ConfiguredClusterFactory();
      configuredClusterFactory.setConf(conf);
      return configuredClusterFactory;
    }
  }
}
