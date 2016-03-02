package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.db.DbKeyValueStore;
import com.airbnb.di.db.StaticDbConnectionFactory;
import com.airbnb.di.utils.EmbeddedMySqlDb;
import com.airbnb.di.utils.ReplicationTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

public class DbKeyValueStoreTest {
  private static final Log LOG = LogFactory.getLog(DbKeyValueStore.class);

  private static EmbeddedMySqlDb embeddedMySqlDb;
  private static String MYSQL_TEST_DB_NAME = "replication_test";
  private static String MYSQL_TEST_TABLE_NAME = "key_value";

  /**
   * TODO.
   *
   * @throws ClassNotFoundException TODO
   * @throws SQLException TODO
   */
  @BeforeClass
  public static void setupClass() throws ClassNotFoundException, SQLException {
    // Create the MySQL process
    embeddedMySqlDb = new EmbeddedMySqlDb();
    embeddedMySqlDb.startDb();

    // Create the DB within MySQL
    Class.forName("com.mysql.jdbc.Driver");
    String username = embeddedMySqlDb.getUsername();
    String password = embeddedMySqlDb.getPassword();
    Connection connection = DriverManager
        .getConnection(ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb), username, password);
    Statement statement = connection.createStatement();
    String sql = "CREATE DATABASE " + MYSQL_TEST_DB_NAME;
    statement.executeUpdate(sql);
    connection.close();
  }

  @Test
  public void testSetAndChangeKey() throws SQLException {
    DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb, MYSQL_TEST_DB_NAME),
        embeddedMySqlDb.getUsername(), embeddedMySqlDb.getPassword());

    // Create the table
    String createTableSql = DbKeyValueStore.getCreateTableSql("key_value");
    Connection connection = dbConnectionFactory.getConnection();
    Statement statement = connection.createStatement();
    statement.execute(createTableSql);

    DbKeyValueStore kvStore = new DbKeyValueStore(dbConnectionFactory, MYSQL_TEST_TABLE_NAME);

    // Set a key, and make sure you get the same value back
    kvStore.set("foo", "bar");
    assertEquals(Optional.of("bar"), kvStore.get("foo"));

    // Change a key, make sure you get the new value
    kvStore.set("foo", "baz");
    assertEquals(Optional.of("baz"), kvStore.get("foo"));

    // Make sure that you get empty for invalid keys
    assertEquals(Optional.empty(), kvStore.get("baz"));

  }

  @AfterClass
  public static void tearDownClass() {
    embeddedMySqlDb.stopDb();
  }
}
