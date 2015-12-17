package com.airbnb.hive;

import com.airbnb.di.hive.hooks.AuditLogHookUtils;
import com.airbnb.di.utils.EmbeddedMySqlDb;
import com.airbnb.di.utils.ReplicationTestUtils;
import com.airbnb.di.utils.TestDbCredentials;
import com.airbnb.di.hive.hooks.HiveOperation;
import com.airbnb.di.hive.hooks.AuditLogHook;
import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.db.StaticDbConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AuditLogHookTest {

    private static final Log LOG = LogFactory.getLog(
            AuditLogHookTest.class);

    private static EmbeddedMySqlDb embeddedMySqlDb;

    private static String DB_NAME = "audit_log_db";
    private static String AUDIT_LOG_TABLE_NAME = "audit_log";
    private static String OUTPUT_OBJECTS_TABLE_NAME = "audit_objects";

    private static final String DEFAULT_QUERY_STRING = "Example query string";
    @BeforeClass
    public static void setupClass() {
        embeddedMySqlDb = new EmbeddedMySqlDb();
        embeddedMySqlDb.startDb();
    }

    public static void resetState() throws IOException, SQLException {
        TestDbCredentials testDbCredentials = new TestDbCredentials();
        DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
                testDbCredentials.getReadWriteUsername(),
                testDbCredentials.getReadWritePassword());
        ReplicationTestUtils.dropDatabase(dbConnectionFactory, DB_NAME);
        AuditLogHookUtils.setupAuditLogTables(dbConnectionFactory,
                DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                OUTPUT_OBJECTS_TABLE_NAME);
    }

    @Test
    public void testAuditLogTable() throws Exception {
        // Setup the audit log DB
        resetState();

        TestDbCredentials testDbCredentials = new TestDbCredentials();
        DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
                testDbCredentials.getReadWriteUsername(),
                testDbCredentials.getReadWritePassword());

        AuditLogHook auditLogHook = new AuditLogHook(testDbCredentials);

        // Set up the source
        org.apache.hadoop.hive.ql.metadata.Table inputTable =
                new org.apache.hadoop.hive.ql.metadata.Table(
                "test_db",
                "test_source_table");
        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        inputTables.add(inputTable);

        org.apache.hadoop.hive.ql.metadata.Table outputTable =
                new org.apache.hadoop.hive.ql.metadata.Table(
                        "test_db",
                        "test_output_table");
        outputTable.setCreateTime(0);

        List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        outputTables.add(outputTable);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.QUERY,
                DEFAULT_QUERY_STRING,
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                outputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                OUTPUT_OBJECTS_TABLE_NAME);

        // Check the query audit log
        List<String> auditLogColumnsToCheck = new ArrayList<String>();
        auditLogColumnsToCheck.add("command_type");
        auditLogColumnsToCheck.add("command");
        auditLogColumnsToCheck.add("inputs");
        auditLogColumnsToCheck.add("outputs");

        List<String> auditLogRow = ReplicationTestUtils.getRow(
                dbConnectionFactory,
                DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                auditLogColumnsToCheck,
                null);

        List<String> expectedDbRow = new ArrayList<String>();
        expectedDbRow.add("QUERY");
        expectedDbRow.add(DEFAULT_QUERY_STRING);
        expectedDbRow.add("{\"tables\":[\"test_db.test_source_table\"]}");
        expectedDbRow.add("{\"tables\":[\"test_db.test_output_table\"]}");
        assertEquals(expectedDbRow, auditLogRow);

        // Check the output objects audit log
        List<String> outputObjectsColumnsToCheck = new ArrayList<String>();
        outputObjectsColumnsToCheck.add("name");
        outputObjectsColumnsToCheck.add("type");
        outputObjectsColumnsToCheck.add("serialized_object");

        List<String> outputObjectsRow = ReplicationTestUtils.getRow(
                dbConnectionFactory,
                DB_NAME,
                OUTPUT_OBJECTS_TABLE_NAME,
                outputObjectsColumnsToCheck,
                null);

        expectedDbRow.clear();
        expectedDbRow.add("test_db.test_output_table");
        expectedDbRow.add("TABLE");
        expectedDbRow.add(
                "{\"1\":{\"str\":\"test_" +
                        "output_table\"},\"2\":{\"str\":\"test_db\"},\"4\":" +
                        "{\"i32\":0},\"5\":{\"i32\":0},\"6\":{\"i3" +
                        "2\":0},\"7\":{\"rec\":{\"1\":{\"lst\":[\"rec\",0]}" +
                        ",\"3\":{\"str\":\"org.apache.hadoop.mapred.Sequenc" +
                        "eFileInputFormat\"},\"4\":{\"str\":\"org.apache.ha" +
                        "doop.hive.ql.io.HiveSequenceFileOutputFormat\"},\"" +
                        "5\":{\"tf\":0},\"6\":{\"i32\":-1},\"7\":{\"rec\":{" +
                        "\"2\":{\"str\":\"org.apache.hadoop.hive.serde2.Met" +
                        "adataTypedColumnsetSerDe\"},\"3\":{\"map\":[\"str\"" +
                        ",\"str\",1,{\"serialization.format\":\"1\"}]}}},\"" +
                        "8\":{\"lst\":[\"str\",0]},\"9\":{\"lst\":[\"rec\"," +
                        "0]},\"10\":{\"map\":[\"str\",\"str\",0,{}]},\"11\"" +
                        ":{\"rec\":{\"1\":{\"lst\":[\"str\",0]},\"2\":{\"ls" +
                        "t\":[\"lst\",0]},\"3\":{\"map\":[\"lst\",\"str\",0" +
                        ",{}]}}}}},\"8\":{\"lst\":[\"rec\",0]},\"9\":{\"map" +
                        "\":[\"str\",\"str\",0,{}]},\"12\":{\"str\":\"MANAG" +
                        "ED_TABLE\"}}");
        assertEquals(expectedDbRow, outputObjectsRow);
    }

    @Test
    public void testAuditLogPartition() throws Exception {
        // Setup the audit log DB
        resetState();

        TestDbCredentials testDbCredentials = new TestDbCredentials();
        DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
                testDbCredentials.getReadWriteUsername(),
                testDbCredentials.getReadWritePassword());

        AuditLogHook auditLogHook = new AuditLogHook(testDbCredentials);


        // Make a partitioned output table
        org.apache.hadoop.hive.ql.metadata.Table qlTable =
                new org.apache.hadoop.hive.ql.metadata.Table(
                        "test_db",
                        "test_output_table");
        List<FieldSchema> partitionCols = new ArrayList<FieldSchema>();
        partitionCols.add(new FieldSchema("ds", null, null));
        qlTable.setPartCols(partitionCols);
        qlTable.setDataLocation(new Path("file://a/b/c"));
        qlTable.setCreateTime(0);

        // Make the actual partition
        Map<String, String> partitionKeyValue = new HashMap<String, String>();
        partitionKeyValue.put("ds", "1");
        org.apache.hadoop.hive.ql.metadata.Partition outputPartition =
                new org.apache.hadoop.hive.ql.metadata.Partition(qlTable,
                        partitionKeyValue, null);
        outputPartition.setLocation("file://a/b/c");
        List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>();
        outputPartitions.add(outputPartition);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.QUERY,
                DEFAULT_QUERY_STRING,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                outputPartitions,
                DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                OUTPUT_OBJECTS_TABLE_NAME);

        // Check the query audit log
        List<String> auditLogColumnsToCheck = new ArrayList<String>();
        auditLogColumnsToCheck.add("command_type");
        auditLogColumnsToCheck.add("command");
        auditLogColumnsToCheck.add("inputs");
        auditLogColumnsToCheck.add("outputs");

        List<String> auditLogRow = ReplicationTestUtils.getRow(
                dbConnectionFactory,
                DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                auditLogColumnsToCheck,
                null);

        List<String> expectedDbRow = new ArrayList<String>();
        expectedDbRow.add("QUERY");
        expectedDbRow.add(DEFAULT_QUERY_STRING);
        expectedDbRow.add("{}");
        expectedDbRow.add("{\"partitions\":" +
                "[\"test_db.test_output_table/ds=1\"]}");
        assertEquals(expectedDbRow, auditLogRow);


        // Check the output objects audit log
        List<String> outputObjectsColumnsToCheck = new ArrayList<String>();
        outputObjectsColumnsToCheck.add("name");
        outputObjectsColumnsToCheck.add("type");
        outputObjectsColumnsToCheck.add("serialized_object");

        List<String> outputObjectsRow = ReplicationTestUtils.getRow(
                dbConnectionFactory,
                DB_NAME,
                OUTPUT_OBJECTS_TABLE_NAME,
                outputObjectsColumnsToCheck,
                "name = 'test_db.test_output_table/ds=1'");

        expectedDbRow.clear();
        expectedDbRow.add("test_db.test_output_table/ds=1");
        expectedDbRow.add("PARTITION");
        expectedDbRow.add("{\"1\":{\"lst\":[\"str\",1,\"1\"]},\"2\":{\"str" +
                        "\":\"test_db\"},\"3\":{\"str\":\"test_output_table" +
                        "\"},\"4\":{\"i32\":0},\"5\":{\"i32\":0},\"6\":{\"rec" +
                        "\":{\"1\":{\"lst\":[\"rec\",0]},\"2\":{\"str\":\"" +
                        "file://a/b/c\"},\"3\":{\"str\":\"org.apache.hadoop." +
                        "mapred.SequenceFileInputFormat\"},\"4\":{\"str\":\"" +
                        "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutput" +
                        "Format\"},\"5\":{\"tf\":0},\"6\":{\"i32\":-1},\"7\"" +
                        ":{\"rec\":{\"2\":{\"str\":\"org.apache.hadoop.hive." +
                        "serde2.MetadataTypedColumnsetSerDe\"},\"3\":{\"map\"" +
                        ":[\"str\",\"str\",1,{\"serialization.format\":\"1\"" +
                        "}]}}},\"8\":{\"lst\":[\"str\",0]},\"9\":{\"lst\":[\"" +
                        "rec\",0]},\"10\":{\"map\":[\"str\",\"str\",0,{}]},\"" +
                        "11\":{\"rec\":{\"1\":{\"lst\":[\"str\",0]},\"2\":{\"" +
                        "lst\":[\"lst\",0]},\"3\":{\"map\":[\"lst\",\"str\",0" +
                        ",{}]}}}}}}");

        assertEquals(expectedDbRow, outputObjectsRow);

        outputObjectsRow = ReplicationTestUtils.getRow(
                dbConnectionFactory,
                DB_NAME,
                OUTPUT_OBJECTS_TABLE_NAME,
                outputObjectsColumnsToCheck,
                "name = 'test_db.test_output_table'");

        expectedDbRow.clear();
        expectedDbRow.add("test_db.test_output_table");
        expectedDbRow.add("TABLE");
        expectedDbRow.add(
                "{\"1\":{\"str\":\"test_output_table\"},\"2\":{\"str\":\"" +
                        "test_db\"},\"4\":{\"i32\":0},\"5\":{\"i3" +
                        "2\":0},\"6\":{\"i32\":0},\"7\":{\"rec\":{\"1\":{\"" +
                        "lst\":[\"rec\",0]},\"2\":{\"str\":\"file://a/b/c\"" +
                        "},\"3\":{\"str\":\"org.apache.hadoop.mapred.Seque" +
                        "nceFileInputFormat\"},\"4\":{\"str\":\"org.apache" +
                        ".hadoop.hive.ql.io.HiveSequenceFileOutputFormat\"" +
                        "},\"5\":{\"tf\":0},\"6\":{\"i32\":-1},\"7\":{\"re" +
                        "c\":{\"2\":{\"str\":\"org.apache.hadoop.hive.serd" +
                        "e2.MetadataTypedColumnsetSerDe\"},\"3\":{\"map\":" +
                        "[\"str\",\"str\",1,{\"serialization.format\":\"1\"" +
                        "}]}}},\"8\":{\"lst\":[\"str\",0]},\"9\":{\"lst\":[" +
                        "\"rec\",0]},\"10\":{\"map\":[\"str\",\"str\",0,{}]" +
                        "},\"11\":{\"rec\":{\"1\":{\"lst\":[\"str\",0]},\"2" +
                        "\":{\"lst\":[\"lst\",0]},\"3\":{\"map\":[\"lst\",\"" +
                        "str\",0,{}]}}}}},\"8\":{\"lst\":[\"rec\",1,{\"1\":" +
                        "{\"str\":\"ds\"}}]},\"9\":{\"map\":[\"str\",\"str\"" +
                        ",0,{}]},\"12\":{\"str\":\"MANAGED_TABLE\"}}");

        assertEquals(expectedDbRow, outputObjectsRow);
    }
    /*
    @Test
    public void testAuditLogTable() throws Exception {
        // Create the audit log DB
        String DB_NAME = "audit_log_db";
        String AUDIT_LOG_TABLE_NAME = "audit_log";
        String OUTPUT_OBJECTS_TABLE_NAME = "output_objects";

        Class.forName("com.mysql.jdbc.Driver");
        String username = embeddedMySqlDb.getUsername();
        String password = embeddedMySqlDb.getPassword();
        Connection connection = DriverManager.getConnection(
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
                username, password);
        Statement statement = connection.createStatement();
        String sql = "CREATE DATABASE audit_log_db";
        statement.executeUpdate(sql);
        connection.close();

        // Recreate the connection using the DB
        String jdbcUrl = ReplicationTestUtils.getJdbcUrl(
                embeddedMySqlDb,
                DB_NAME);
        connection = DriverManager.getConnection(jdbcUrl,
                embeddedMySqlDb.getUsername(),
                embeddedMySqlDb.getPassword());
        statement = connection.createStatement();

        // Create the audit log tables
        String createAuditLogTableSql =  "CREATE TABLE `audit_log` (" +
                "`id` bigint(20) NOT NULL AUTO_INCREMENT, " +
                "`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                "`query_id` varchar(256) DEFAULT NULL," +
                "`command_type` varchar(64) DEFAULT NULL," +
                "`command` mediumtext," +
                "`inputs` mediumtext," +
                "`outputs` mediumtext," +
                "`username` varchar(64) DEFAULT NULL," +
                "`chronos_job_name` varchar(256) DEFAULT NULL," +
                "`chronos_job_owner` varchar(256) DEFAULT NULL," +
                "`mesos_task_id` varchar(256) DEFAULT NULL," +
                "`ip` varchar(64) DEFAULT NULL," +
                "`extras` mediumtext," +
                "PRIMARY KEY (`id`)," +
                "KEY `ts_index` (`ts`)" +
                ") ENGINE=InnoDB";

        statement.execute(createAuditLogTableSql);

        // Create the output table
        String outputTableCreate = "CREATE TABLE `output_objects` (" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT, " +
                "  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                "  `audit_log_id` bigint(20) NOT NULL, " +
                "  `output_type` varchar(64) DEFAULT NULL, " +
                "  `output` varchar(4000) DEFAULT NULL, " +
                "  `output_serialized` mediumtext, " +
                "  PRIMARY KEY (`id`), " +
                "  KEY `ts_index` (`ts`) " +
                "  ) ENGINE=InnoDB";

        statement.execute(outputTableCreate);


        AuditLogHook auditLogHook = new AuditLogHook(new TestDbCredentials());

        // Set up the source
        org.apache.hadoop.hive.ql.metadata.Table srcTable =
                new org.apache.hadoop.hive.ql.metadata.Table(
                        "test_db",
                        "test_source_table");

        Set<ReadEntity> readEntities = new HashSet<ReadEntity>();
        readEntities.add(new ReadEntity(srcTable));

        org.apache.hadoop.hive.ql.metadata.Table insertTable =
                new org.apache.hadoop.hive.ql.metadata.Table(
                        "test_db",
                        "test_output_table");

        Set<WriteEntity> writeEntities = new HashSet<WriteEntity>();
        writeEntities.add(new WriteEntity(insertTable));

        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getUserName()).thenReturn("test_user");

        HiveConf hiveConf = new HiveConf();
        SessionState sessionState = new SessionState(hiveConf);
        sessionState.setCmd("INSERT OVEWRITE TABLE test_output_table " +
                "SELECT * FROM test_source_table");
        sessionState.setCommandType(HiveOperation.QUERY);

        hiveConf.set(AuditLogHook.JDBC_URL_KEY,
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb, DB_NAME));
        hiveConf.set(AuditLogHook.TABLE_NAME_KEY, AUDIT_LOG_TABLE_NAME);
        hiveConf.set(AuditLogHook.OBJECT_TABLE_NAME_KEY,
                OUTPUT_OBJECTS_TABLE_NAME);

        // Run the log
        auditLogHook.run(sessionState, readEntities, writeEntities, null, ugi);

        // Check the query audit log
        List<String> auditLogColumnsToCheck = new ArrayList<String>();
        auditLogColumnsToCheck.add("command_type");
        auditLogColumnsToCheck.add("command");
        auditLogColumnsToCheck.add("inputs");
        auditLogColumnsToCheck.add("outputs");

        List<String> auditLogRow = ReplicationTestUtils.getRow(jdbcUrl, username,
                password,
                AUDIT_LOG_TABLE_NAME, auditLogColumnsToCheck);

        List<String> expectedDbRow = new ArrayList<String>();
        expectedDbRow.add("QUERY");
        expectedDbRow.add("INSERT OVEWRITE TABLE test_output_table " +
                "SELECT * FROM test_source_table");
        expectedDbRow.add("{\"tables\":[\"test_db.test_source_table\"]}");
        expectedDbRow.add("{\"tables\":[\"test_db.test_output_table\"]}");
        assertEquals(expectedDbRow, auditLogRow);

        // Check the output objects audit log
        List<String> outputObjectsColumnsToCheck = new ArrayList<String>();
        outputObjectsColumnsToCheck.add("output");
        outputObjectsColumnsToCheck.add("output_type");
        outputObjectsColumnsToCheck.add("output_serialized");

        List<String> outputObjectsRow = ReplicationTestUtils.getRow(jdbcUrl,
                username, password, OUTPUT_OBJECTS_TABLE_NAME,
                outputObjectsColumnsToCheck);

        expectedDbRow.clear();
        expectedDbRow.add("test_db.test_output_table");
                expectedDbRow.add("TABLE");
        expectedDbRow.add(
                "{\"1\":{\"str\":\"test_output_table\"},\"2\":{\"str\"" +
                        ":\"test_db\"},\"4\":{\"i32\":0},\"5\":{\"i32\":0}" +
                        ",\"6\":{\"i32\":0},\"7\":{\"rec\":{\"1\":{\"lst\":" +
                        "[\"rec\",0]},\"3\":{\"str\":\"" +
                        "org.apache.hadoop.mapred.SequenceFileInputFormat\"}," +
                        "\"4\":{\"str\":\"org.apache.hadoop.hive.ql.io." +
                        "HiveSequenceFileOutputFormat\"},\"5\":{\"tf\":0},\"" +
                        "6\":{\"i32\":-1},\"7\":{\"rec\":{\"2\":{\"str\":\"" +
                        "org.apache.hadoop.hive.serde2." +
                        "MetadataTypedColumnsetSerDe\"},\"3\":{\"map\"" +
                        ":[\"str\",\"str\",1,{\"serialization.format\"" +
                        ":\"1\"}]}}},\"8\":{\"lst\":[\"str\",0]},\"9\":" +
                        "{\"lst\":[\"rec\",0]},\"10\":{\"map\":[\"str\",\"" +
                        "str\",0,{}]},\"11\":{\"rec\":{\"1\":{\"lst\":[\"" +
                        "str\",0]},\"2\":{\"lst\":[\"lst\",0]},\"3\":{\"" +
                        "map\":[\"lst\",\"str\",0,{}]}}}}},\"8\":{\"lst\"" +
                        ":[\"rec\",0]},\"9\":{\"map\":[\"str\",\"str\",0," +
                        "{}]},\"12\":{\"str\":\"MANAGED_TABLE\"}}");
        assertEquals(expectedDbRow, outputObjectsRow);
    }
    */

    @AfterClass
    public static void tearDownClass() {
        embeddedMySqlDb.stopDb();
    }
}
