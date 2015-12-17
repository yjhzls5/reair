package com.airbnb.di.hive.common;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


public interface HiveMetastoreClient {

    public Partition addPartition(Partition p)
            throws HiveMetastoreException;

    public Table getTable(String dbName,
                                       String tableName)
            throws HiveMetastoreException;

    public Partition getPartition(String dbName, String tableName,
                                               String partitionName)
            throws HiveMetastoreException;

    public List<String> getPartitionNames(String dbName, String tableName)
        throws HiveMetastoreException;

    public void alterPartition(String dbName, String tableName,
                                            Partition p)
            throws HiveMetastoreException;

    public void alterTable(String dbName, String tableName,
                                        Table t)
            throws HiveMetastoreException;

    public boolean isPartitioned(String dbName, String tableName)
            throws HiveMetastoreException;

    public boolean existsPartition(String dbName, String tableName,
                                                String partitionName)
            throws HiveMetastoreException;

    public boolean existsTable(String dbName, String tableName)
            throws HiveMetastoreException;

    public void createTable(Table t)
            throws HiveMetastoreException;

    public void dropTable(String dbName, String tableName,
                                       boolean deleteData)
            throws HiveMetastoreException;

    public void dropPartition(String dbName, String tableName,
                                           String partitionName,
                                           boolean deleteData)
            throws HiveMetastoreException;

    public Map<String, String> partitionNameToMap(
            String partitionName)
            throws HiveMetastoreException;

    public void createDatabase(Database db) throws HiveMetastoreException;

    public Database getDatabase(String dbName) throws HiveMetastoreException;

    public boolean existsDb(String dbName) throws HiveMetastoreException;

    public List<String> getTables(String dbName, String tableName) throws
            HiveMetastoreException;

    public Partition exchangePartition(Map<String, String> partitionSpecs,
                                       String sourceDb,
                                       String sourceTable,
                                       String destDb,
                                       String destinationTableName)
        throws HiveMetastoreException;

    public void renamePartition(String db,
                                String table,
                                List<String> partitionValues,
                                Partition p)
        throws HiveMetastoreException;
}
