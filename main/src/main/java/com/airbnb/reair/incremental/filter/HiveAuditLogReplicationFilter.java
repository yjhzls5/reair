package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * To filter out Thrift events from the audit log.
 */
public class HiveAuditLogReplicationFilter implements ReplicationFilter {

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean accept(AuditLogEntry entry) {
    switch (entry.getCommandType()) {
      case THRIFT_ADD_PARTITION:
      case THRIFT_ALTER_PARTITION:
      case THRIFT_ALTER_TABLE:
      case THRIFT_CREATE_DATABASE:
      case THRIFT_CREATE_TABLE:
      case THRIFT_DROP_DATABASE:
      case THRIFT_DROP_PARTITION:
      case THRIFT_DROP_TABLE:
        return false;

      default:
        return true;
    }
  }

  @Override
  public boolean accept(Table table) {
    return accept(table, null);
  }

  @Override
  public boolean accept(Table table, NamedPartition partition) {
    return true;
  }
}
