package com.airbnb.di.hive.batchreplication.metastore;

import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.Map;

public class HiveMetastoreClient {

    private static int DEFAULT_SOCKET_TIMEOUT = 1000;

    private String host;
    private int port;
    private int clientSocketTimeout;

    private TTransport transport;
    private ThriftHiveMetastore.Client client;

    public HiveMetastoreClient(String host, int port)
            throws HiveMetastoreException {
        this.host = host;
        this.port = port;
        this.clientSocketTimeout = DEFAULT_SOCKET_TIMEOUT;

        connect();
    }

    private void connect() throws HiveMetastoreException {

        transport = new TSocket(host,
                port,
                1000 * clientSocketTimeout);

        this.client =
                new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));

        try {
            transport.open();
        } catch (TTransportException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void close() {
        transport.close();
    }

    public Table getTable(String dbName,
                          String tableName)
            throws HiveMetastoreException {

        try {
            return client.get_table(dbName, tableName);
        } catch (NoSuchObjectException e) {
            return null;
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public Partition getPartition(String dbName, String tableName,
                                  String partitionName)
            throws HiveMetastoreException {

        try {
            return client.get_partition_by_name(dbName, tableName,
                    partitionName);
        } catch (NoSuchObjectException e) {
            return null;
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public List<Partition> getAllPartitions(String dbName, String tableName)
            throws HiveMetastoreException {

        try {
            return client.get_partitions(dbName, tableName, (short) -1);
        } catch (NoSuchObjectException e) {
            return null;
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public List<String> getPartitionNames(String dbName, String tableName)
            throws HiveMetastoreException {

        try {
            return client.get_partition_names(dbName, tableName, (short) -1);
        } catch (NoSuchObjectException e) {
            return null;
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void alterPartition(String dbName, String tableName, Partition p)
            throws HiveMetastoreException {
        try {
            client.alter_partition(dbName, tableName, p);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void alterTable(String dbName, String tableName, Table t)
            throws HiveMetastoreException {
        try {
            client.alter_table(dbName, tableName, t);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public boolean isPartitioned(String dbName, String tableName)
            throws HiveMetastoreException {
        Table t = getTable(dbName, tableName);
        return t != null && t.getPartitionKeys().size() > 0;
    }

    public boolean existsPartition(String dbName, String tableName,
                                   String partitionName)
            throws HiveMetastoreException {
        return getPartition(dbName, tableName, partitionName) != null;
    }

    public boolean existsTable(String dbName, String tableName)
            throws HiveMetastoreException {
        return getTable(dbName, tableName) != null;
    }

    public void createTable(Table t) throws HiveMetastoreException {
        try {
            client.create_table(t);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void dropTable(String dbName, String tableName, boolean deleteData)
            throws HiveMetastoreException {
        try {
            client.drop_table(dbName, tableName, deleteData);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public Map<String, String> partitionNameToMap(String partitionName)
            throws HiveMetastoreException {
        try {
            return client.partition_name_to_spec(partitionName);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public List<String> getAllDatabases() throws HiveMetastoreException {
        try {
            return client.get_all_databases();
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public List<String> getAllTables(String db_name) throws HiveMetastoreException {
        try {
            return client.get_all_tables(db_name);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void dropPartition(String dbName, String tableName, String partitionName, boolean deleteData)
            throws HiveMetastoreException {
        try {
            client.drop_partition_by_name(dbName, tableName, partitionName, deleteData);
        } catch (NoSuchObjectException e) {
            // if partition does not exist, ignore it.
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void createPartition(Partition p) throws HiveMetastoreException {
        try {
            client.add_partition(p);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public Database getDatabase(String dbName)
            throws HiveMetastoreException {

        try {
            return client.get_database(dbName);
        } catch (NoSuchObjectException e) {
            return null;
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    public void createDatabase(Database d) throws HiveMetastoreException {
        try {
            client.create_database(d);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }
}