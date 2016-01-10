package test;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.hive.replication.PersistedJobInfo;
import com.airbnb.di.hive.replication.PersistedJobInfoStore;
import com.airbnb.di.hive.replication.ReplicationOperation;
import com.airbnb.di.hive.replication.ReplicationStatus;
import com.airbnb.di.db.StaticDbConnectionFactory;
import com.airbnb.di.utils.EmbeddedMySqlDb;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class PersistedJobInfoStoreTest {
    private static final Log LOG = LogFactory.getLog(
            PersistedJobInfoStoreTest.class);

    private static EmbeddedMySqlDb embeddedMySqlDb;
    private static String MYSQL_TEST_DB_NAME = "replication_test";
    private static String MYSQL_TEST_TABLE_NAME = "replication_jobs";

    @BeforeClass
    public static void setupClass() throws ClassNotFoundException, SQLException{
        // Create the MySQL process
        embeddedMySqlDb = new EmbeddedMySqlDb();
        embeddedMySqlDb.startDb();

        // Create the DB within MySQL
        Class.forName("com.mysql.jdbc.Driver");
        String username = embeddedMySqlDb.getUsername();
        String password = embeddedMySqlDb.getPassword();
        Connection connection = DriverManager.getConnection(
                ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
                username, password);
        Statement statement = connection.createStatement();
        String sql = "CREATE DATABASE " + MYSQL_TEST_DB_NAME;
        statement.executeUpdate(sql);
        connection.close();
    }

    @Test
    public void testCreateAndUpdate() throws IOException, SQLException {
        // Create the state table
        DbConnectionFactory dbConnectionFactory =
                new StaticDbConnectionFactory(
                        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb,
                        MYSQL_TEST_DB_NAME),
                        embeddedMySqlDb.getUsername(),
                        embeddedMySqlDb.getPassword());



        Connection connection = dbConnectionFactory.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(PersistedJobInfoStore.getCreateTableSql(
                "replication_jobs"));
        PersistedJobInfoStore jobStore = new PersistedJobInfoStore(
                dbConnectionFactory,
                MYSQL_TEST_TABLE_NAME);


        // Test out creation
        List<String> partitionNames = new ArrayList<String>();
        partitionNames.add("ds=1/hr=1");
        Map<String, String> extras = new HashMap<String, String>();
        extras.put("foo", "bar");
        PersistedJobInfo testJob = jobStore.resilientCreate(
                ReplicationOperation.COPY_UNPARTITIONED_TABLE,
                ReplicationStatus.PENDING,
                Optional.of(new Path("file:///tmp/test_table")),
                "src_cluster",
                new HiveObjectSpec("test_db", "test_table", "ds=1/hr=1"),
                partitionNames,
                Optional.of("1"),
                Optional.of(new HiveObjectSpec("test_db",
                        "renamed_table",
                        "ds=1/hr=1")),
                Optional.of(new Path("file://tmp/a/b/c")),
                extras);

        // Test out retrieval
        Map<Long, PersistedJobInfo> idToJob = new HashMap<Long, PersistedJobInfo>();
        List<PersistedJobInfo> persistedJobInfos = jobStore.getRunnableFromDb();
        for (PersistedJobInfo persistedJobInfo : persistedJobInfos) {
            idToJob.put(Long.valueOf(persistedJobInfo.getId()), persistedJobInfo);
        }

        // Make sure that the job that was created is the same as the job that
        // was retrieved
        assertEquals(testJob, idToJob.get(testJob.getId()));

        // Try modifying the job
        testJob.setStatus(ReplicationStatus.RUNNING);
        jobStore.persist(testJob);

        // Verify that the change is retrieved
        idToJob.clear();
        persistedJobInfos = jobStore.getRunnableFromDb();
        for (PersistedJobInfo persistedJobInfo : persistedJobInfos) {
            idToJob.put(Long.valueOf(persistedJobInfo.getId()), persistedJobInfo);
        }
        assertEquals(testJob, idToJob.get(testJob.getId()));
    }

    @AfterClass
    public static void tearDownClass() {
        embeddedMySqlDb.stopDb();
    }
}
