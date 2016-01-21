package com.airbnb.di.hive.replication.configuration;

import org.apache.hadoop.conf.Configuration;

public interface ClusterFactory {

  void setConf(Configuration conf);

  Cluster getSrcCluster() throws ConfigurationException;

  Cluster getDestCluster() throws ConfigurationException;
}
