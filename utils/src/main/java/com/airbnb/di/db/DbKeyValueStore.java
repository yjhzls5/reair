package com.airbnb.di.db;

import com.airbnb.di.common.Container;
import com.airbnb.di.utils.RetryableTask;
import com.airbnb.di.utils.RetryingTaskRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class DbKeyValueStore {

  private static final Log LOG = LogFactory.getLog(DbKeyValueStore.class);

  private DbConnectionFactory dbConnectionFactory;
  private String dbTableName;
  private RetryingTaskRunner retryingTaskRunner = new RetryingTaskRunner();

  public DbKeyValueStore(DbConnectionFactory dbConnectionFactory, String dbTableName)
      throws SQLException {

    this.dbTableName = dbTableName;
    this.dbConnectionFactory = dbConnectionFactory;
  }

  public static String getCreateTableSql(String tableName) {
    return String.format("CREATE TABLE `%s` (\n" + "  `update_time` timestamp NOT NULL DEFAULT "
        + "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "  `key_string` varchar(256) NOT NULL,\n"
        + "  `value_string` varchar(4000) DEFAULT NULL,\n" + "  PRIMARY KEY (`key_string`)\n"
        + ") ENGINE=InnoDB", tableName);
  }

  public Optional<String> resilientGet(final String value) throws SQLException {
    final Container<Optional<String>> ret = new Container<>();
    retryingTaskRunner.runUntilSuccessful(new RetryableTask() {
      @Override
      public void run() throws Exception {
        ret.set(get(value));
      }
    });
    return ret.get();
  }

  public Optional<String> get(String value) throws SQLException {
    Connection connection = dbConnectionFactory.getConnection();
    String query =
        String.format("SELECT value_string FROM %s " + "WHERE key_string = ? LIMIT 1", dbTableName);
    PreparedStatement ps = connection.prepareStatement(query);
    try {
      ps.setString(1, value);
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        return Optional.ofNullable(rs.getString(1));
      } else {
        return Optional.empty();
      }
    } finally {
      ps.close();
      ps = null;
    }
  }

  public void resilientSet(final String key, final String value) {
    retryingTaskRunner.runUntilSuccessful(new RetryableTask() {
      @Override
      public void run() throws Exception {
        set(key, value);
      }
    });
  }

  public void set(String key, String value) throws SQLException {
    LOG.debug("Setting " + key + " to " + value);
    Connection connection = dbConnectionFactory.getConnection();
    String query = String.format("INSERT INTO %s (key_string, value_string) "
        + "VALUE (?, ?) ON DUPLICATE KEY UPDATE value_string = ?", dbTableName);

    PreparedStatement ps = connection.prepareStatement(query);
    try {
      ps.setString(1, key);
      ps.setString(2, value);
      ps.setString(3, value);
      ps.executeUpdate();
    } finally {
      ps.close();
      ps = null;
    }
  }

  /*
   * public static int main(String [] args) throws Exception { String jdbcUrl =
   * "jdbc:mysql://pinkybrain-hive.cqmqbyzxdwlk." +
   * "us-east-1.rds.amazonaws.com:3306/hive_replication"; String dbUser =
   * DbCredentials.getUsername(); String dbPass = DbCredentials.getPassword();
   * 
   * DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(jdbcUrl, dbUser,
   * dbPass); DbKeyValueStore kvStore = new DbKeyValueStore(dbConnectionFactory, "key_value");
   * 
   * kvStore.set("foo", "bar"); System.out.println("fruit is " + kvStore.get("fruit"));
   * System.out.println("vegetable is " + kvStore.get("vegetable")); System.out.println("foo is " +
   * kvStore.get("foo")); System.out.println("bar is " + kvStore.get("bar")); return 0; }
   */
}
