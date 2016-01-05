package com.airbnb.di.hive.replication.filter;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.NamedPartition;
import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;

public class TestReplicationFilter implements ReplicationFilter {

    private static final Log LOG = LogFactory.getLog(TestReplicationFilter.class);

    @Override
    public boolean accept(AuditLogEntry entry) {
        return true;
    }

    @Override
    public boolean accept(Table table) {
        if (TableType.VIRTUAL_VIEW.toString().equals(
                table.getTableType())) {
            LOG.warn(String.format("Filtering %s since it's a view",
                    new HiveObjectSpec(table)));
            return false;
        }
        return "paul_yang".equals(table.getDbName());
    }

    @Override
    public boolean accept(Table table, NamedPartition partition) {
        if (TableType.VIRTUAL_VIEW.toString().equals(
                table.getTableType())) {
            LOG.warn(String.format("Filtering %s since it's a view",
                    new HiveObjectSpec(table)));
            return false;
        }
        return "paul_yang".equals(table.getDbName());
    }

    @Override
    public void setConf(Configuration conf) {

    }
}
