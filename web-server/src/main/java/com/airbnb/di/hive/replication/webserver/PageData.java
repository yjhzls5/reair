package com.airbnb.di.hive.replication.webserver;

import com.airbnb.di.hive.replication.thrift.TReplicationJob;
import com.airbnb.di.hive.replication.thrift.TReplicationService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;

public class PageData {
  private String host;
  private int port;
  private List<TReplicationJob> activeJobs = null;
  private List<TReplicationJob> retiredJobs = null;
  private long lag;

  public PageData(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public void fetchData() throws TException {
    TTransport transport;

    transport = new TSocket(host, port);
    transport.open();

    try {
      TProtocol protocol = new TBinaryProtocol(transport);
      TReplicationService.Client client = new TReplicationService.Client(protocol);

      retiredJobs = new ArrayList<>();
      long marker = -1;
      while (true) {
        List<TReplicationJob> jobBatch = client.getRetiredJobs(marker, 100);
        if (jobBatch.size() == 0) {
          break;
        }
        retiredJobs.addAll(jobBatch);
        // The marker should be the id of the last job
        marker = jobBatch.get(jobBatch.size() - 1).getId();
      }


      // Get the active jobs
      activeJobs = new ArrayList<>();
      marker = -1;
      while (true) {
        List<TReplicationJob> jobBatch = client.getActiveJobs(marker, 100);
        if (jobBatch.size() == 0) {
          break;
        }
        activeJobs.addAll(jobBatch);
        // The marker should be the id of the last job
        marker = jobBatch.get(jobBatch.size() - 1).getId();
      }

      // Get the lag
      lag = client.getLag();
    } finally {
      transport.close();
    }
  }

  public List<TReplicationJob> getActiveJobs() {
    return activeJobs;
  }

  public List<TReplicationJob> getRetiredJobs() {
    return retiredJobs;
  }

  public long getLag() {
    return lag;
  }
}
