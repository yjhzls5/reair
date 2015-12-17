package com.airbnb.di.utils;

import com.airbnb.di.common.PathBuilder;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveParameterKeys;
import com.airbnb.di.db.DbConnectionFactory;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by paul_yang on 7/14/15.
 */
public class ReplicationTestUtils {

    private static final Log LOG = LogFactory.getLog(
            ReplicationTestUtils.class);


    private static Path getPathForHiveObject(Path warehouseRoot,
                                             HiveObjectSpec spec) {
        PathBuilder pb = new PathBuilder(warehouseRoot);
        pb.add(spec.getDbName());
        pb.add(spec.getTableName());
        if (spec.isPartition()) {
            pb.add(spec.getPartitionName());
        }
        return pb.toPath();
    }

    private static void createSomeTextFiles(Configuration conf, Path directory)
            throws IOException {
        Path file1Path = new Path(directory, "file1.txt");
        Path file2Path = new Path(directory, "file2.txt");
        FileSystem fs = FileSystem.get(file1Path.toUri(), conf);

        FSDataOutputStream file1OutputStream = fs.create(file1Path);
        file1OutputStream.writeBytes("foobar");
        file1OutputStream.close();

        FSDataOutputStream file2OutputStream = fs.create(file2Path);
        file2OutputStream.writeBytes("123");
        file2OutputStream.close();
    }


    /**
     * Creates an unpartitioned table with some dummy files
     *
     * @param conf
     * @param ms
     * @param tableSpec
     * @param warehouseRoot
     * @return
     * @throws IOException
     * @throws HiveMetastoreException
     */
    public static Table createUnpartitionedTable(Configuration conf,
                                                 HiveMetastoreClient ms,
                                                 HiveObjectSpec tableSpec,
                                                 TableType tableType,
                                                 Path warehouseRoot)
            throws IOException, HiveMetastoreException {

        // Set up the basic properties of the table
        Table t = new Table();
        t.setDbName(tableSpec.getDbName());
        t.setTableName(tableSpec.getTableName());
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(HiveParameterKeys.TLDT, Long.toString(
                System.currentTimeMillis()));
        t.setParameters(parameters);
        t.setPartitionKeys(new ArrayList<FieldSchema>());
        t.setTableType(tableType.toString());

        // Setup the columns and the storage descriptor
        StorageDescriptor sd = new StorageDescriptor();
        // Set the schema for the table
        List<FieldSchema> columns = new ArrayList<FieldSchema>();
        columns.add(new FieldSchema("key", "string",
                "my comment"));
        sd.setCols(columns);

        if (tableType == TableType.MANAGED_TABLE ||
                tableType == TableType.EXTERNAL_TABLE) {
            Path tableLocation = getPathForHiveObject(warehouseRoot, tableSpec);
            sd.setLocation(tableLocation.toString());
            // Make some fake files
            createSomeTextFiles(conf, tableLocation);
        } else if (tableType == TableType.VIRTUAL_VIEW) {
            t.setTableType(TableType.VIRTUAL_VIEW.toString());
        }
        t.setSd(sd);

        // Create DB for table if one does not exist
        if (!ms.existsDb(t.getDbName())) {
            ms.createDatabase(new Database(t.getDbName(), null, null, null));
        }
        ms.createTable(t);

        return t;
    }

    /**
     * Creates a table that is partitioned on ds and hr
     * @param conf
     * @param ms
     * @param tableSpec
     * @param warehouseRoot
     * @return
     * @throws IOException
     * @throws HiveMetastoreException
     */
    public static Table createPartitionedTable(Configuration conf,
                                        HiveMetastoreClient ms,
                                        HiveObjectSpec tableSpec,
                                        TableType tableType,
                                        Path warehouseRoot)
            throws IOException, HiveMetastoreException {
        Path tableLocation = getPathForHiveObject(warehouseRoot, tableSpec);

        // Set up the basic properties of the table
        Table t = new Table();
        t.setDbName(tableSpec.getDbName());
        t.setTableName(tableSpec.getTableName());
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(HiveParameterKeys.TLDT, Long.toString(
                System.currentTimeMillis()));
        t.setParameters(parameters);
        t.setTableType(tableType.toString());

        // Set up the partitioning scheme
        List<FieldSchema> partitionCols = new ArrayList<FieldSchema>();
        partitionCols.add(new FieldSchema("ds", "string", "my ds comment"));
        partitionCols.add(new FieldSchema("hr", "string", "my hr comment"));
        t.setPartitionKeys(partitionCols);

        // Setup the columns and the storage descriptor
        StorageDescriptor sd = new StorageDescriptor();
        // Set the schema for the table
        List<FieldSchema> columns = new ArrayList<FieldSchema>();
        columns.add(new FieldSchema("key", "string",
                "my comment"));
        sd.setCols(columns);
        if (tableType == TableType.MANAGED_TABLE ||
                tableType == TableType.EXTERNAL_TABLE) {
            sd.setLocation(tableLocation.toString());
        }

        sd.setSerdeInfo(new SerDeInfo("LazySimpleSerde",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                new HashMap<String, String>()));
        t.setSd(sd);

        // Create DB for table if one does not exist
        if (!ms.existsDb(t.getDbName())) {
            ms.createDatabase(new Database(t.getDbName(), null, null, null));
        }

        ms.createTable(t);

        return t;
    }

    /**
     * Creates a partition in a table with some dummy files
     *
     * @param conf
     * @param ms
     * @param partitionSpec
     * @return
     * @throws IOException
     * @throws HiveMetastoreException
     */
    public static Partition createPartition(Configuration conf,
                                            HiveMetastoreClient ms,
                                            HiveObjectSpec partitionSpec)
            throws IOException, HiveMetastoreException {

        HiveObjectSpec tableSpec = partitionSpec.getTableSpec();
        if (! ms.existsTable(tableSpec.getDbName(),
                tableSpec.getTableName())) {
            throw new HiveMetastoreException("Missing table " + tableSpec);
        }
        Table t = ms.getTable(tableSpec.getDbName(), tableSpec.getTableName());

        Partition p = new Partition();
        p.setDbName(partitionSpec.getDbName());
        p.setTableName(partitionSpec.getTableName());

        Map<String, String> partitionKeyValues = ms.partitionNameToMap(
                partitionSpec.getPartitionName());
        p.setValues(Lists.newArrayList(partitionKeyValues.values()));
        StorageDescriptor psd = new StorageDescriptor(t.getSd());
        TableType tableType = TableType.valueOf(t.getTableType());
        if (tableType.equals(TableType.MANAGED_TABLE) ||
                tableType.equals(TableType.EXTERNAL_TABLE)) {
            // Make the location for the partition to be in a subdirectory of the
            // table location. String concatenation here is not great.
            String partitionLocation = t.getSd().getLocation() + "/" +
                    partitionSpec.getPartitionName();
            psd.setLocation(partitionLocation);
            createSomeTextFiles(conf, new Path(partitionLocation));
        }

        // Set the serde info as it can cause an NPE otherwise when creating
        // ql Partition objects.
        psd.setSerdeInfo(new SerDeInfo("LazySimpleSerde",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                new HashMap<String, String>()));
        p.setSd(psd);

        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(HiveParameterKeys.TLDT, Long.toString(
                System.currentTimeMillis()));
        p.setParameters(parameters);

        ms.addPartition(p);
        return p;
    }

    public static void updateModifiedTime(HiveMetastoreClient ms,
                                          HiveObjectSpec objectSpec)
            throws HiveMetastoreException {
        if (objectSpec.isPartition()) {
            Partition p = ms.getPartition(objectSpec.getDbName(),
                    objectSpec.getTableName(), objectSpec.getPartitionName());
            p.getParameters().put(HiveParameterKeys.TLDT,
                    Long.toString(System.currentTimeMillis()));
        } else {
            Table t = ms.getTable(objectSpec.getDbName(),
                    objectSpec.getTableName());
            t.getParameters().put(HiveParameterKeys.TLDT,
                    Long.toString(System.currentTimeMillis()));
        }

    }

    public static String getModifiedTime(HiveMetastoreClient ms,
                                       HiveObjectSpec objectSpec)
            throws HiveMetastoreException {
        if (objectSpec.isPartition()) {
            Partition p = ms.getPartition(objectSpec.getDbName(),
                    objectSpec.getTableName(), objectSpec.getPartitionName());
            return p.getParameters().get(HiveParameterKeys.TLDT);
        } else {
            Table t = ms.getTable(objectSpec.getDbName(),
                    objectSpec.getTableName());
            return t.getParameters().get(HiveParameterKeys.TLDT);
        }
    }

    public static List<String> getRow(String jdbcUrl, String username,
                                      String password,
                                      String tableName,
                                      List<String> columnNames)
            throws ClassNotFoundException, SQLException
    {
        return getRow(jdbcUrl, username, password, tableName, columnNames, null);
    }

    public static List<String> getRow(String jdbcUrl,
                                      String username,
                                      String password,
                                      String tableName,
                                      List<String> columnNames,
                                      String whereClause)
            throws ClassNotFoundException, SQLException
    {
        StringBuilder qb = new StringBuilder();

        List<String> columnExpressions = new ArrayList<String>();
        for (String columnName : columnNames) {
            columnExpressions.add(String.format("CAST(%s AS CHAR)", columnName));
        }


        qb.append("SELECT ");
        qb.append(StringUtils.join(", ", columnExpressions));

        qb.append(" FROM ");
        qb.append(tableName);
        if (whereClause != null) {
            qb.append(" WHERE ");
            qb.append(whereClause);
        }

        LOG.info("Running query " + qb.toString());

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(jdbcUrl, username,
                password);

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(qb.toString());

        List<String> row = null;

        if (rs.next()) {
            row = new ArrayList<String>();
            for (int i=1; i <= columnNames.size(); i++) {
                row.add(rs.getString(i));
            }
        }
        connection.close();
        return row;
    }

    public static List<String> getRow(DbConnectionFactory dbConnectionFactory,
                                      String dbName,
                                      String tableName,
                                      List<String> columnNames,
                                      String whereClause)
            throws ClassNotFoundException, SQLException
    {
        StringBuilder qb = new StringBuilder();

        List<String> columnExpressions = new ArrayList<String>();
        for (String columnName : columnNames) {
            columnExpressions.add(String.format("CAST(%s AS CHAR)", columnName));
        }


        qb.append("SELECT ");
        qb.append(StringUtils.join(", ", columnExpressions));

        qb.append(" FROM ");
        qb.append(tableName);
        if (whereClause != null) {
            qb.append(" WHERE ");
            qb.append(whereClause);
        }

        LOG.info("Running query " + qb.toString());

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = dbConnectionFactory.getConnection();
        connection.setCatalog(dbName);

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(qb.toString());

        List<String> row = null;

        if (rs.next()) {
            row = new ArrayList<String>();
            for (int i=1; i <= columnNames.size(); i++) {
                row.add(rs.getString(i));
            }
        }
        connection.close();
        return row;
    }

    public static String getJdbcUrl(EmbeddedMySqlDb db) {
        return String.format("jdbc:mysql://%s:%s/",
                db.getHost(), db.getPort());
    }

    public static String getJdbcUrl(EmbeddedMySqlDb db, String dbName) {
        return String.format("jdbc:mysql://%s:%s/%s",
                db.getHost(), db.getPort(), dbName);
    }

    public static void dropDatabase(DbConnectionFactory connectionFactory,
                                    String dbName) throws SQLException{
        Connection connection = connectionFactory.getConnection();
        String sql = String.format("DROP DATABASE IF EXISTS %s", dbName);
        Statement statement = connection.createStatement();
        try {
            statement.execute(sql);
        } finally {
            statement.close();
            connection.close();
        }
    }
}
