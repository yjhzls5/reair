package com.airbnb.di.hive.replication.deploy;

import com.airbnb.di.hive.common.NamedPartition;
import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;
import com.airbnb.di.hive.replication.filter.ReplicationFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

public class GoldToSilverTestReplicationFilter implements ReplicationFilter {

  @Override
  public void setConf(Configuration conf) {
    return;
  }

  @Override
  public boolean accept(AuditLogEntry entry) {
    return true;
  }

  @Override
  public boolean accept(Table table) {
    // return "gold_to_silver_replication".equals(table.getDbName());
    // return "core_data".equals(table.getDbName());

    if (table.getTableName().startsWith("staging")) {
      return false;
    }

    if (table.getDbName().startsWith("tmp")) {
      return false;
    }

    if (table.getTableName().indexOf("_schema_upgrade") != -1) {
      return false;
    }

    /*
     * if ("core_data".equals(table.getDbName())) { return true; }
     * 
     * if ("db_exports".equals(table.getDbName())) { return true; }
     */

    return true;
  }

  @Override
  public boolean accept(Table table, NamedPartition partition) {
    return accept(table);
  }
}
