package com.airbnb.di.hive.replication.configuration;

import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public interface ReplicationFilter {

    public boolean accept(AuditLogEntry entry);

    public boolean accept(Table table);

    public boolean accept(Table table, Partition partition);
}
