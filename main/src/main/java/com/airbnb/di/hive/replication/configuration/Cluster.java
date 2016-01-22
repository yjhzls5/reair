package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.ThriftHiveMetastoreClient;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates information about a cluster - generally a HDFS, MR, and a Hive metastore that are
 * considered as a unit.
 */
public interface Cluster {
  public HiveMetastoreClient getMetastoreClient() throws HiveMetastoreException;

  public Path getFsRoot();

  public Path getTmpDir();

  public String getName();
}
