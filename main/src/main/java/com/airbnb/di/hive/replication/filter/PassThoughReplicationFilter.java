package com.airbnb.di.hive.replication.filter;

import com.airbnb.di.hive.common.NamedPartition;
import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;
import com.airbnb.di.hive.replication.filter.ReplicationFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

public class PassThoughReplicationFilter implements ReplicationFilter {
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
        return true;
    }

    @Override
    public boolean accept(Table table, NamedPartition partition) {
        return true;
    }
}
