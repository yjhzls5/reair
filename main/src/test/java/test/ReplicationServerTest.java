package test;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveParameterKeys;
import com.airbnb.di.hive.common.HiveUtils;
import com.airbnb.di.hive.hooks.AuditLogHookUtils;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.utils.EmbeddedMySqlDb;
import com.airbnb.di.utils.ReplicationTestUtils;
import com.airbnb.di.utils.TestDbCredentials;
import com.airbnb.di.hive.hooks.HiveOperation;
import com.airbnb.di.hive.hooks.AuditLogHook;
import com.airbnb.di.hive.replication.auditlog.AuditLogReader;
import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.db.DbKeyValueStore;
import com.airbnb.di.hive.replication.configuration.PassThoughReplicationFilter;
import com.airbnb.di.hive.replication.PersistedJobInfoStore;
import com.airbnb.di.hive.replication.configuration.ReplicationFilter;
import com.airbnb.di.hive.replication.ReplicationServer;
import com.airbnb.di.db.StaticDbConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicationServerTest extends MockClusterTest {

    private static final Log LOG = LogFactory.getLog(
            ReplicationServerTest.class);

    private static EmbeddedMySqlDb embeddedMySqlDb;

    private static final String AUDIT_LOG_DB_NAME = "audit_log_db";
    private static final String AUDIT_LOG_TABLE_NAME = "audit_log";
    private static final String AUDIT_LOG_OBJECTS_TABLE_NAME = "audit_objects";

    private static final String REPLICATION_STATE_DB_NAME =
            "replication_state_db";
    private static final String KEY_VALUE_TABLE_NAME = "key_value";
    private static final String REPLICATION_JOB_STATE_TABLE_NAME =
            "replication_state";

    private static final String HIVE_DB = "test_db";

    private static AuditLogHook auditLogHook;
    private static AuditLogReader auditLogReader;
    private static DbKeyValueStore dbKeyValueStore;
    private static PersistedJobInfoStore persistedJobInfoStore;
    private static ReplicationFilter replicationFilter;

    @BeforeClass
    public static void setupClass() throws IOException, SQLException {
        MockClusterTest.setupClass();
        embeddedMySqlDb = new EmbeddedMySqlDb();
        embeddedMySqlDb.startDb();

        resetState();
    }

    public static void resetState() throws IOException, SQLException {
        TestDbCredentials testDbCredentials = new TestDbCredentials();
        DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
                testDbCredentials.getReadWriteUsername(),
                testDbCredentials.getReadWritePassword());

        // Drop the databases to start fresh
        ReplicationTestUtils.dropDatabase(dbConnectionFactory,
                AUDIT_LOG_DB_NAME);
        ReplicationTestUtils.dropDatabase(dbConnectionFactory,
                REPLICATION_STATE_DB_NAME);

        // Create the audit log DB and tables
        AuditLogHookUtils.setupAuditLogTables(dbConnectionFactory,
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);

        // Recreate the connection factory so that it uses the database
        DbConnectionFactory auditLogDbConnectionFactory =
                new StaticDbConnectionFactory(
                        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb,
                                AUDIT_LOG_DB_NAME),
                        testDbCredentials.getReadWriteUsername(),
                        testDbCredentials.getReadWritePassword());

        auditLogHook = new AuditLogHook(testDbCredentials);

        // Setup the DB and tables needed to store replication state
        setupReplicationServerStateTables(dbConnectionFactory,
                REPLICATION_STATE_DB_NAME,
                KEY_VALUE_TABLE_NAME,
                REPLICATION_JOB_STATE_TABLE_NAME);

        DbConnectionFactory replicationStateDbConnectionFactory =
                new StaticDbConnectionFactory(
                        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb,
                                REPLICATION_STATE_DB_NAME),
                        testDbCredentials.getReadWriteUsername(),
                        testDbCredentials.getReadWritePassword());

        auditLogReader = new AuditLogReader(
                auditLogDbConnectionFactory,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME,
                0);

        dbKeyValueStore = new DbKeyValueStore(
                replicationStateDbConnectionFactory,
                KEY_VALUE_TABLE_NAME);

        persistedJobInfoStore =
                new PersistedJobInfoStore(
                        replicationStateDbConnectionFactory,
                        REPLICATION_JOB_STATE_TABLE_NAME);

        replicationFilter = new PassThoughReplicationFilter();
    }

    public static void clearMetastores() throws HiveMetastoreException {
        // Drop all tables from the destination metastore
        for (String tableName : srcMetastore.getTables(HIVE_DB, "*")) {
            srcMetastore.dropTable(HIVE_DB, tableName, true);
        }
        // Drop all partitions from the destination metastore
        for (String tableName : destMetastore.getTables(HIVE_DB, "*")) {
            destMetastore.dropTable(HIVE_DB, tableName, true);
        }
    }

    public static void setupReplicationServerStateTables(
            DbConnectionFactory dbConnectionFactory,
            String dbName,
            String keyValueTableName,
            String persistedJobInfoTableName) throws SQLException {
        String createDbSql = String.format("CREATE DATABASE %s", dbName);
        String createKeyValueTableSql =
                DbKeyValueStore.getCreateTableSql(keyValueTableName);
        String createPersistedJobInfoTable =
                PersistedJobInfoStore.getCreateTableSql(
                        persistedJobInfoTableName);

        Connection connection = dbConnectionFactory.getConnection();

        Statement statement = connection.createStatement();

        // Create the tables
        try {
            statement.execute(createDbSql);

            connection.setCatalog(dbName);

            statement = connection.createStatement();
            statement.execute(createKeyValueTableSql);
            statement.execute(createPersistedJobInfoTable);
        } finally {
            statement.close();
            connection.close();
        }
    }

    @Test
    public void testTableReplication() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        // Create an unpartitioned table in the source and a corresponding
        // entry in the audit log
        String dbName = "test_db";
        String tableName = "test_table";
        simulatedCreateUnpartitionedTable(dbName, tableName);

        // Have the replication server copy it.
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);

        replicationServer.run(1);

        // Verify that the object was copied
        assertTrue(destMetastore.existsTable(dbName, tableName));
    }

    @Test
    public void testPartitionReplication() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        // Create an partitioned table in the source and a corresponding
        // entry in the audit log
        String dbName = "test_db";
        String tableName = "test_table";
        String partitionName = "ds=1/hr=2";

        simulateCreatePartitionedTable(dbName, tableName);
        simulateCreatePartition(dbName, tableName, partitionName);

        // Have the replication server copy it.
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);

        replicationServer.run(2);

        // Verify that the object was copied
        assertTrue(destMetastore.existsTable(dbName, tableName));
        assertTrue(destMetastore.existsPartition(
                dbName,
                tableName,
                partitionName));
    }

    @Test
    public void testOptimizedPartitionReplication() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        // Create an partitioned table in the source and a corresponding
        // entry in the audit log
        String dbName = "test_db";
        String tableName = "test_table";
        List<String> partitionNames = new ArrayList<String>();
        partitionNames.add("ds=1/hr=1");
        partitionNames.add("ds=1/hr=2");
        partitionNames.add("ds=1/hr=3");

        simulateCreatePartitionedTable(dbName, tableName);
        simulateCreatePartitions(dbName, tableName, partitionNames);

        // Have the replication server copy it.
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);

        replicationServer.run(2);
        LOG.error("Server stopped");
        // Verify that the object was copied
        assertTrue(destMetastore.existsTable(dbName, tableName));

        for (String partitionName : partitionNames) {
            assertTrue(destMetastore.existsPartition(
                    dbName,
                    tableName,
                    partitionName));
        }
    }

    private void removeTableAttributes(
            List<org.apache.hadoop.hive.ql.metadata.Table> tables) {
        for (org.apache.hadoop.hive.ql.metadata.Table table : tables) {
            Table newTable = new Table(table.getTTable());
            newTable.setParameters(null);
            table.setTTable(newTable);
        }
    }

    private void removePartitionAttributes(
            List<org.apache.hadoop.hive.ql.metadata.Partition> partitions) {
        for (org.apache.hadoop.hive.ql.metadata.Partition p : partitions) {
            Table newTable = new Table(p.getTable().getTTable());
            Partition newPartition = new Partition(p.getTPartition());
            newTable.setParameters(null);

            newPartition.setParameters(null);

            p.getTable().setTTable(newTable);
            p.setTPartition(newPartition);
        }
    }

    private void simulatedCreateUnpartitionedTable(String dbName,
                                                   String tableName)
            throws Exception {
        // Create an unpartitioned table in the source and a corresponding
        // entry in the audit log
        HiveObjectSpec unpartitionedTable = new HiveObjectSpec(dbName,
                tableName);
        Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
                srcMetastore,
                unpartitionedTable,
                TableType.MANAGED_TABLE,
                srcWarehouseRoot);

        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        outputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
        removeTableAttributes(outputTables);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.QUERY,
                "Example query string",
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                outputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }

    private void simulatedRenameTable(String dbName,
                                      String oldTableName,
                                      String newTableName)
            throws Exception {
        Table srcTable = srcMetastore.getTable(dbName, oldTableName);
        Table renamedTable = new Table(srcTable);
        renamedTable.setTableName(newTableName);
        srcMetastore.alterTable(dbName, oldTableName, renamedTable);

        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        org.apache.hadoop.hive.ql.metadata.Table qlSrcTable =
                new org.apache.hadoop.hive.ql.metadata.Table(srcTable);
        inputTables.add(qlSrcTable);

        List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        outputTables.add(qlSrcTable);
        org.apache.hadoop.hive.ql.metadata.Table qlRenamedTable =
                new org.apache.hadoop.hive.ql.metadata.Table(renamedTable);
        outputTables.add(qlRenamedTable);


        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.ALTERTABLE_RENAME,
                "Example query string",
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                outputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }

    private void simulateCreatePartitionedTable(String dbName,
                                                String tableName)
            throws Exception {
        // Create an unpartitioned table in the source and a corresponding
        // entry in the audit log
        HiveObjectSpec unpartitionedTable = new HiveObjectSpec(dbName,
                tableName);
        Table srcTable = ReplicationTestUtils.createPartitionedTable(conf,
                srcMetastore,
                unpartitionedTable,
                TableType.MANAGED_TABLE,
                srcWarehouseRoot);

        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        outputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
        removeTableAttributes(outputTables);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.QUERY,
                "Example query string",
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                outputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);

    }

    private void simulateCreatePartition(String dbName,
                                         String tableName,
                                         String partitionName)
            throws Exception {
        HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName,
                partitionName);

        Table srcTable = srcMetastore.getTable(dbName, tableName);
        Partition srcPartition = ReplicationTestUtils.createPartition(conf,
                srcMetastore,
                partitionSpec);

        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>();

        inputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
        outputPartitions.add(new org.apache.hadoop.hive.ql.metadata.Partition(
                new org.apache.hadoop.hive.ql.metadata.Table(srcTable),
                srcPartition));

        removeTableAttributes(inputTables);
        removePartitionAttributes(outputPartitions);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.QUERY,
                "Example query string",
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                outputPartitions,
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }

    private void simulateCreatePartitions(String dbName,
                                          String tableName,
                                          List<String> partitionNames)
            throws Exception {
        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>();


        Table srcTable = srcMetastore.getTable(dbName, tableName);
        inputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));

        for (String partitionName : partitionNames) {
            HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName,
                    partitionName);

            Partition srcPartition = ReplicationTestUtils.createPartition(conf,
                    srcMetastore,
                    partitionSpec);

            outputPartitions.add(new org.apache.hadoop.hive.ql.metadata.Partition(
                    new org.apache.hadoop.hive.ql.metadata.Table(srcTable),
                    srcPartition));
        }

        removeTableAttributes(inputTables);
        removePartitionAttributes(outputPartitions);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.QUERY,
                "Example query string",
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                outputPartitions,
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }


    private void simulateDropTable(String dbName,
                                   String tableName)
            throws Exception {
        // Drop the specified table from the source and also generate the
        // appropriate audit log entry
        HiveObjectSpec tableSpec = new HiveObjectSpec(dbName, tableName);
        Table srcTable = srcMetastore.getTable(dbName, tableName);
        srcMetastore.dropTable(dbName, tableName, false);

        List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        outputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
        removeTableAttributes(outputTables);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.DROPTABLE,
                "Example query string",
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                outputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }

    private void simulateDropPartition(String dbName,
                                       String tableName,
                                       String partitionName)
            throws Exception {
        // Drop the specified partition from the source and also generate the
        // appropriate audit log entry
        Table srcTable = srcMetastore.getTable(dbName, tableName);
        Partition srcPartition = srcMetastore.getPartition(dbName, tableName,
                partitionName);
        srcMetastore.dropPartition(dbName, tableName, partitionName, false);

        List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>();
        org.apache.hadoop.hive.ql.metadata.Table qlTable =
                new org.apache.hadoop.hive.ql.metadata.Table(srcTable);
        inputTables.add(qlTable);

        List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>();
        outputPartitions.add(
                new org.apache.hadoop.hive.ql.metadata.Partition(qlTable,
                        srcPartition));

        removeTableAttributes(inputTables);
        removePartitionAttributes(outputPartitions);

        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                HiveOperation.ALTERTABLE_DROPPARTS,
                "Example query string",
                inputTables,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                outputPartitions,
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }

    private void simulateExchangePartition(String exchangeFromDbName,
                                           String exchangeFromTableName,
                                           String exchangeToDbName,
                                           String exchangeToTableName,
                                           String partitionName)
            throws Exception {

        // Do the exchange
        srcMetastore.exchangePartition(
                srcMetastore.partitionNameToMap(partitionName),
                exchangeFromDbName,
                exchangeFromTableName,
                exchangeToDbName,
                exchangeToTableName);

        String query = String.format("ALTER TABLE %s.%s EXCHANGE " +
                "%s WITH TABLE %s.%s",
                exchangeFromDbName,
                exchangeFromTableName,
                HiveUtils.partitionNameToDdlSpec(partitionName),
                exchangeToDbName,
                exchangeToTableName);

        // Generate the broken audit log entry. Hive should be fixed to have the
        // correct entry. It's broken in that the command type is null and
        // inputs and outputs are empty
        AuditLogHookUtils.insertAuditLogEntry(embeddedMySqlDb,
                auditLogHook,
                null,
                query,
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Table>(),
                new ArrayList<org.apache.hadoop.hive.ql.metadata.Partition>(),
                AUDIT_LOG_DB_NAME,
                AUDIT_LOG_TABLE_NAME,
                AUDIT_LOG_OBJECTS_TABLE_NAME);
    }

    /**
     * Tests to make sure that entries that were not completed in the previous
     * invocation of the server are picked up and run on a subsequent
     * invocation.
     * @throws Exception
     */
    @Test
    public void testResumeJobs() throws Exception{
        // Reset the state
        resetState();
        clearMetastores();

        String dbName = "test_db";
        String firstTableName = "test_table_1";
        String secondTableName = "test_table_2";

        // Create the objects and the audit log entry
        simulatedCreateUnpartitionedTable(dbName, firstTableName);
        simulatedCreateUnpartitionedTable(dbName, secondTableName);

        // Have the replication server copy the first table
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                2,
                null);
        replicationServer.run(1);

        // Verify that the object was copied
        assertTrue(destMetastore.existsTable(dbName, firstTableName));
        assertFalse(destMetastore.existsTable(dbName, secondTableName));

        // Re-run. Since the last run finished the first entry, the second run
        // should copy the second entry.
        replicationServer.run(1);

        // Verify that the second object was copied
        assertTrue(destMetastore.existsTable(dbName, firstTableName));
        assertTrue(destMetastore.existsTable(dbName, secondTableName));
    }

    @Test
    public void testDropPartition() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        String dbName = "test_db";
        String tableName = "test_table";
        String partitionName = "ds=1/hr=2";
        HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName,
                partitionName);

        // Create a partitioned table and a partition on the source, and
        // replicate it.
        simulateCreatePartitionedTable(dbName, tableName);
        simulateCreatePartition(dbName, tableName, partitionName);
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);
        replicationServer.run(2);

        // Verify that the partition is on the destination
        assertTrue(destMetastore.existsPartition(dbName, tableName,
                partitionName));

        // Simulate the drop
        LOG.info("Dropping " + partitionSpec);
        simulateDropPartition(dbName, tableName, partitionName);

        // Run replication so that it picks up the drop command
        replicationServer.run(1);

        // Verify that the partition is gone from the destination
        assertFalse(destMetastore.existsPartition(dbName, tableName,
                partitionName));
    }


    @Test
    public void testDropTable() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        String dbName = "test_db";
        String tableName = "test_table";

        // Create a table on the source, and replicate it
        simulatedCreateUnpartitionedTable(dbName, tableName);
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);
        replicationServer.run(1);

        // Verify that the partition is on the destination
        assertTrue(destMetastore.existsTable(dbName, tableName));
        // Simulate the drop
        simulateDropTable(dbName, tableName);

        // Run replication so that it picks up the drop command
        replicationServer.run(1);

        // Verify that the partition is gone from the destination
        assertFalse(destMetastore.existsTable(dbName, tableName));
    }


    /**
     * Test to make sure that the drop table command does not get replicated
     * if the table is modified on the destination.
     * @throws Exception
     */
    @Test
    public void testDropTableNoOp() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        String dbName = "test_db";
        String tableName = "test_table";
        String secondTableName = "test_table_2";

        // Create a table on the source, and replicate it
        simulatedCreateUnpartitionedTable(dbName, tableName);
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);
        replicationServer.run(1);

        // Verify that the partition is on the destination
        assertTrue(destMetastore.existsTable(dbName, tableName));
        // Simulate the drop
        simulateDropTable(dbName, tableName);

        // Update the modified time on the destination table
        Table table = destMetastore.getTable(dbName, tableName);
        table.getParameters().put(HiveParameterKeys.TLDT,
                Long.toString(System.currentTimeMillis()));
        destMetastore.alterTable(dbName, tableName, table);

        // Create another table on the source so that replication has something
        // to do on the next invocation if it skips the drop command
        simulatedCreateUnpartitionedTable(dbName, secondTableName);

        // Run replication so that it picks up the drop command
        replicationServer.run(1);

        // Verify that the partition is still there on the destination
        assertTrue(destMetastore.existsTable(dbName, tableName));
    }

    /**
     * Test whether the rename table operation is properly propagated.
     * @throws Exception
     */
    @Test
    public void testRenameTable() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        String dbName = "test_db";
        String tableName = "test_table";
        String newTableName = "new_test_table";

        // Create a table on the source, and replicate it
        simulatedCreateUnpartitionedTable(dbName, tableName);
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);
        replicationServer.run(1);

        // Verify that the table is on the destination
        assertTrue(destMetastore.existsTable(dbName, tableName));

        // Simulate the rename
        simulatedRenameTable(dbName, tableName, newTableName);

        // Propagate the rename
        replicationServer.run(1);

        // Verify that the partition is still there on the destination
        assertFalse(destMetastore.existsTable(dbName, tableName));
        assertTrue(destMetastore.existsTable(dbName, newTableName));
    }

    /**
     * Test whether the rename table operation is properly propagated in case
     * when the table is updated on the destination. In such a case, the
     * table should be copied over.
     * @throws Exception
     */
    @Test
    public void testRenameTableCopy() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        String dbName = "test_db";
        String tableName = "test_table";
        String newTableName = "new_test_table";

        // Create a table on the source, and replicate it
        simulatedCreateUnpartitionedTable(dbName, tableName);
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1, null);
        replicationServer.run(1);

        // Verify that the table is on the destination
        assertTrue(destMetastore.existsTable(dbName, tableName));

        // Update the modified time on the destination table
        Table table = destMetastore.getTable(dbName, tableName);
        table.getParameters().put(HiveParameterKeys.TLDT,
                Long.toString(System.currentTimeMillis()));
        destMetastore.alterTable(dbName, tableName, table);

        // Simulate the rename
        simulatedRenameTable(dbName, tableName, newTableName);

        // Propagate the rename
        replicationServer.run(1);

        // Verify that the renamed table was copied over, and the modified table
        // remains.
        assertTrue(destMetastore.existsTable(dbName, tableName));
        assertTrue(destMetastore.existsTable(dbName, newTableName));
    }

    @Test
    public void testExchangePartition() throws Exception {
        // Reset the state
        resetState();
        clearMetastores();

        // Create an partitioned table in the source and a corresponding
        // entry in the audit log
        String dbName = "test_db";
        String exchangeFromTableName = "test_table_exchange_from";
        String exchangeToTableName = "test_table_exchange_to";
        String partitionName = "ds=1/hr=2";

        simulateCreatePartitionedTable(dbName, exchangeFromTableName);
        simulateCreatePartition(dbName, exchangeFromTableName, partitionName);
        simulateCreatePartitionedTable(dbName, exchangeToTableName);
        simulateExchangePartition(dbName,
                exchangeFromTableName,
                dbName,
                exchangeToTableName,
                partitionName);

        // Have the replication server copy it.
        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                replicationFilter,
                new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
                1,
                1,
                null);

        replicationServer.run(4);

        // Verify that the object was copied
        assertTrue(destMetastore.existsTable(dbName, exchangeFromTableName));
        assertTrue(destMetastore.existsTable(dbName, exchangeToTableName));
        assertTrue(destMetastore.existsPartition(
                dbName,
                exchangeToTableName,
                partitionName));
    }

    @AfterClass
    public static void tearDownClass() {
        MockClusterTest.tearDownClass();
        embeddedMySqlDb.stopDb();
    }



    // Additional cases to test - restore copy partition, copy unpartitioned table
    // Filtering out tables / partitions from renames
    // Table partition scheme changes
}
