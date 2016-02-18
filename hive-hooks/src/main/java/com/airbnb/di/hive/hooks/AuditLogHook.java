package com.airbnb.di.hive.hooks;

import com.airbnb.di.db.DbCredentials;
import com.airbnb.di.utils.RetryableTask;
import com.airbnb.di.utils.RetryingTaskRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Set;

/**
 * A post-execute hook that writes information about successfully executed
 * queries into a MySQL DB. Every successful query generates an entry in an
 * audit log table. In addition, every output object for a successful query
 * generates entries in the output objects table, and the map reduce stats
 * tables.
 */
public class AuditLogHook implements PostExecute {

  public static Logger LOG = Logger.getLogger(AuditLogHook.class);

  // Number of attempts to make
  private static int NUM_ATTEMPTS = 10;
  // Will wait BASE_SLEEP * 2 ^ (attempt no.) between attempts
  private static int BASE_SLEEP = 1;

  public static String DB_USERNAME =
      "airbnb.hive.audit_log.db.username";
  public static String DB_PASSWORD =
      "airbnb.hive.audit_log.db.password";
  // Keys for values in hive-site.xml
  public static String JDBC_URL_KEY = "airbnb.logging.audit_log.jdbc_url";

  protected DbCredentials dbCreds;

  public AuditLogHook() {
  }

  // Constructor used for testing
  public AuditLogHook(DbCredentials dbCreds) {
    this.dbCreds = dbCreds;
  }

  protected DbCredentials getDbCreds(Configuration conf) {
    if (dbCreds == null) {
      dbCreds = new ConfigurationDbCredentials(conf, DB_USERNAME, DB_PASSWORD);
    }
    return dbCreds;
  }

  @Override
  public void run(final SessionState sessionState,
                  final Set<ReadEntity> readEntities,
                  final Set<WriteEntity> writeEntities,
                  final LineageInfo lineageInfo,
                  final UserGroupInformation userGroupInformation)
      throws Exception {
    HiveConf conf = sessionState.getConf();

    final DbCredentials dbCreds = getDbCreds(conf);

    final String jdbcUrl = conf.get(JDBC_URL_KEY);
    if (jdbcUrl == null) {
      throw new ConfigurationException(JDBC_URL_KEY
          + " is not defined in the conf!");
    }

    RetryingTaskRunner runner = new RetryingTaskRunner(NUM_ATTEMPTS,
        BASE_SLEEP);

    long startTime = System.currentTimeMillis();
    LOG.debug("Starting insert into audit log");
    runner.runWithRetries(new RetryableTask() {
      @Override
      public void run() throws Exception {
        Connection connection = DriverManager.getConnection(jdbcUrl,
            dbCreds.getReadWriteUsername(),
            dbCreds.getReadWritePassword());
        connection.setTransactionIsolation(
            Connection.TRANSACTION_READ_COMMITTED);

        // Turn off auto commit so that we can ensure that both the
        // audit log entry and the output rows appear at the same time.
        connection.setAutoCommit(false);

        runLogModules(
            connection,
            sessionState,
            readEntities,
            writeEntities,
            userGroupInformation);

        connection.commit();
      }
    });
    LOG.debug(String.format("Applying log modules took %d ms",
        System.currentTimeMillis() - startTime));
  }

  /**
   * Runs the individual audit log modules that make up this hook.
   *
   * @param connection TODO
   * @param sessionState TODO
   * @param readEntities TODO
   * @param writeEntities TODO
   * @param userGroupInformation TODO
   * @return the id column in sql for the core audit log entry for the query
   *
   * @throws Exception TODO
   */
  protected long runLogModules(final Connection connection,
                             final SessionState sessionState,
                             final Set<ReadEntity> readEntities,
                             final Set<WriteEntity> writeEntities,
                             final UserGroupInformation userGroupInformation)
      throws Exception {
    long auditLogId = new AuditCoreLogModule(
                              connection,
                              sessionState,
                              readEntities,
                              writeEntities,
                              userGroupInformation).run();
    new ObjectLogModule(
            connection,
            sessionState,
            readEntities,
            writeEntities,
            auditLogId).run();
    new MapRedStatsLogModule(
            connection,
            sessionState,
            auditLogId).run();

    return auditLogId;
  }
}
