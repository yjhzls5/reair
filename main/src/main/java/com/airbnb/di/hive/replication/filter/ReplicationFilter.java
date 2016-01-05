package com.airbnb.di.hive.replication.filter;

import com.airbnb.di.hive.common.NamedPartition;
import com.airbnb.di.hive.replication.auditlog.AuditLogEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public interface ReplicationFilter {

    void setConf(Configuration conf);

    boolean accept(AuditLogEntry entry);

    boolean accept(Table table);

    boolean accept(Table table, NamedPartition partition);
}
