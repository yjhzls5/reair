package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.hive.replication.DirectoryCopier;
import org.apache.hadoop.conf.Configuration;

public interface ClusterFactory {

  void setConf(Configuration conf);

  Cluster getSrcCluster() throws ConfigurationException;

  Cluster getDestCluster() throws ConfigurationException;

  DirectoryCopier getDirectoryCopier() throws ConfigurationException;
}
