package com.airbnb.di.hive.common;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.Map;

public class ThriftHiveMetastoreClient implements HiveMetastoreClient {

    private static int DEFAULT_SOCKET_TIMEOUT = 600;

    private String host;
    private int port;
    private int clientSocketTimeout;

    private TTransport transport;
    private ThriftHiveMetastore.Client client;

    public ThriftHiveMetastoreClient(String host, int port)
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

    private void close() {
        transport.close();
    }

    synchronized public Partition addPartition(Partition p)
            throws HiveMetastoreException {
        try {
            return client.add_partition(p);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    synchronized public Table getTable(String dbName,
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

    synchronized public Partition getPartition(String dbName, String tableName,
                                  String partitionName)
            throws HiveMetastoreException {

        try {
            return client.get_partition_by_name(dbName, tableName,
                    partitionName);
        } catch (NoSuchObjectException e) {
            return null;
        } catch (MetaException e) {
            // Brittle code - this was added to handle an issue with the Hive
            // Metstore. The MetaException is thrown when a table is
            // partitioned with one schema but the name follows a different one.
            // It's impossible to differentiate from that case and other
            // causes of the MetaException without something like this.
            if ("Invalid partition key & values".equals(e.getMessage())) {
                return null;
            } else {
                throw new HiveMetastoreException(e);
            }
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    synchronized public void alterPartition(String dbName, String tableName,
                                            Partition p)
            throws HiveMetastoreException {
        try {
            client.alter_partition(dbName, tableName, p);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    synchronized public void alterTable(String dbName, String tableName,
                                        Table t)
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

    synchronized public boolean existsPartition(String dbName, String tableName,
                                                String partitionName)
            throws HiveMetastoreException {
        return getPartition(dbName, tableName, partitionName) != null;
    }

    synchronized public boolean existsTable(String dbName, String tableName)
            throws HiveMetastoreException {
        return getTable(dbName, tableName) != null;
    }

    synchronized public void createTable(Table t)
            throws HiveMetastoreException {
        try {
            client.create_table(t);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    synchronized public void dropTable(String dbName, String tableName,
                                       boolean deleteData)
            throws HiveMetastoreException {
        try {
            client.drop_table(dbName, tableName, deleteData);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    synchronized public void dropPartition(String dbName, String tableName,
                                           String partitionName,
                                           boolean deleteData)
            throws HiveMetastoreException {
        try {
            client.drop_partition_by_name(dbName, tableName, partitionName,
                    deleteData);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    synchronized public Map<String, String> partitionNameToMap(
            String partitionName)
            throws HiveMetastoreException {
        try {
            return client.partition_name_to_spec(partitionName);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    @Override
    synchronized public void createDatabase(Database db)
            throws HiveMetastoreException {
        try {
            client.create_database(db);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    @Override
    synchronized public Database getDatabase(String dbName)
            throws HiveMetastoreException {
        try {
            return client.get_database(dbName);
        } catch (NoSuchObjectException e) {
          return null;
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    @Override
    synchronized  public boolean existsDb(String dbName)
            throws HiveMetastoreException {
        return getDatabase(dbName) != null;
    }

    @Override
    synchronized public List<String> getPartitionNames(String dbName,
                                                       String tableName)
            throws HiveMetastoreException {
        try {
            return client.get_partition_names(dbName, tableName, (short)-1);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    @Override
    synchronized public List<String> getTables(String dbName, String tableName)
            throws HiveMetastoreException {
        try {
            return client.get_tables(dbName, tableName);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    @Override
    synchronized public Partition exchangePartition(Map<String, String> partitionSpecs,
                                                    String sourceDb,
                                                    String sourceTable,
                                                    String destDb,
                                                    String destinationTableName)
            throws HiveMetastoreException {
        try {
            return client.exchange_partition(partitionSpecs,
                    sourceDb,
                    sourceTable,
                    destDb,
                    destinationTableName);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }

    @Override
    public void renamePartition(String db,
                                String table,
                                List<String> partitionValues, Partition p)
            throws HiveMetastoreException {
        try {
            client.rename_partition(db,
                    table,
                    partitionValues,
                    p);
        } catch (TException e) {
            throw new HiveMetastoreException(e);
        }
    }
}
