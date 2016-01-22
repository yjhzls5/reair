package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.ThriftHiveMetastoreClient;
import com.airbnb.di.hive.replication.configuration.Cluster;
import org.apache.hadoop.fs.Path;

/**
 * A cluster defined with manual hard coded values.
 */
public class HardCodedCluster implements Cluster {

  private String name;
  private String metastoreHost;
  private int metastorePort;
  private String jobtrackerHost;
  private String jobtrackerPort;
  private Path hdfsRoot;
  private Path tmpDir;

  /**
   * TODO.
   *
   * @param name TODO
   * @param metastoreHost TODO
   * @param metastorePort TODO
   * @param jobtrackerHost TODO
   * @param jobtrackerPort TODO
   * @param hdfsRoot TODO
   * @param tmpDir TODO
   */
  public HardCodedCluster(
      String name,
      String metastoreHost,
      int metastorePort,
      String jobtrackerHost,
      String jobtrackerPort,
      Path hdfsRoot,
      Path tmpDir) {
    this.name = name;
    this.metastoreHost = metastoreHost;
    this.metastorePort = metastorePort;
    this.jobtrackerHost = jobtrackerHost;
    this.jobtrackerPort = jobtrackerPort;
    this.hdfsRoot = hdfsRoot;
    this.tmpDir = tmpDir;
  }

  public String getMetastoreHost() {
    return metastoreHost;
  }

  public int getMetastorePort() {
    return metastorePort;
  }

  public ThriftHiveMetastoreClient getMetastoreClient() throws HiveMetastoreException {
    return new ThriftHiveMetastoreClient(getMetastoreHost(), getMetastorePort());
  }

  public String getJobtrackerHost() {
    return jobtrackerHost;
  }

  public String getJobtrackerPort() {
    return jobtrackerPort;
  }

  public Path getFsRoot() {
    return hdfsRoot;
  }

  public Path getTmpDir() {
    return tmpDir;
  }

  public String getName() {
    return name;
  }
}
