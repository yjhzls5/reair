package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.hive.hooks.HiveOperation;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * To replicate thrift events and also ALTERTABLE_EXCHANGEPARTITION
 * from audit log.
 */
public class ThriftLogReplicationFilter implements ReplicationFilter {

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
      // for completeness we need to replicate exchange partition from
      // the normal audit log besides the thrift events
      case ALTERTABLE_EXCHANGEPARTITION:
        return true;

      default:
        return false;
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
