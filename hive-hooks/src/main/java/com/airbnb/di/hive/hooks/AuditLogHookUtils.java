package com.airbnb.di.hive.hooks;

import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.utils.EmbeddedMySqlDb;
import com.airbnb.di.utils.ReplicationTestUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuditLogHookUtils {
  /**
   * In the MySQL DB, setup the DB and the tables for the audit log to work
   * properly.
   * @param connectionFactory TODO
   * @param dbName TODO
   * @param auditCoreLogTableName TODO
   * @param objectsTableName TODO
   * @param mapRedStatsTableName TODO
   * @throws java.sql.SQLException TODO
   */
  public static void setupAuditLogTables(
      DbConnectionFactory connectionFactory,
      String dbName,
      String auditCoreLogTableName,
      String objectsTableName,
      String mapRedStatsTableName) throws SQLException {

    // Define the SQL that will do the creation
    String createDbSql = String.format("CREATE DATABASE %s", dbName);

    String createAuditLogTableSql =  String.format(
        "CREATE TABLE `%s` ("
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "`query_id` varchar(256) DEFAULT NULL,"
            + "`command_type` varchar(64) DEFAULT NULL,"
            + "`command` mediumtext,"
            + "`inputs` mediumtext,"
            + "`outputs` mediumtext,"
            + "`username` varchar(64) DEFAULT NULL,"
            + "`ip` varchar(64) DEFAULT NULL,"
            + "`extras` mediumtext,"
            + "PRIMARY KEY (`id`),"
            + "KEY `create_time_index` (`create_time`)"
            + ") ENGINE=InnoDB", auditCoreLogTableName);

    String createObjectsTableSql = String.format(
        "CREATE TABLE `%s` ("
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "`audit_log_id` bigint(20) NOT NULL, "
            + "`category` varchar(64) DEFAULT NULL, "
            + "`type` varchar(64) DEFAULT NULL, "
            + "`name` varchar(4000) DEFAULT NULL, "
            + "`serialized_object` mediumtext, "
            + "PRIMARY KEY (`id`), "
            + "KEY `create_time_index` (`create_time`) "
            + ") ENGINE=InnoDB", objectsTableName);

    String createMapRedStatsTableSql = String.format(
        "CREATE TABLE `%s` ("
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "`audit_log_id` bigint(20) NOT NULL, "
            + "`stage` varchar(256) NOT NULL, "
            + "`mappers` bigint(20) NOT NULL, "
            + "`reducers` bigint(20) NOT NULL, "
            + "`cpu_time` bigint(20) NOT NULL, "
            + "`counters` text(240000) DEFAULT NULL, "
            + "PRIMARY KEY (`id`), "
            + "KEY `create_time_index` (`create_time`) ,"
            + "KEY `audit_log_id_index` (`audit_log_id`) "
            + ") ENGINE=InnoDB", mapRedStatsTableName);

    // Create the database
    Connection connection = connectionFactory.getConnection();

    Statement statement = connection.createStatement();

    // Create the tables
    try {
      statement.execute(createDbSql);

      connection.setCatalog(dbName);

      statement = connection.createStatement();
      statement.execute(createAuditLogTableSql);
      statement.execute(createObjectsTableSql);
      statement.execute(createMapRedStatsTableSql);
    } finally {
      statement.close();
      connection.close();
    }
  }

  /**
   * TODO.
   *
   * @param auditLogHook TODO
   * @param operation TODO
   * @param command TODO
   * @param inputTables TODO
   * @param inputPartitions TODO
   * @param outputTables TODO
   * @param outputPartitions TODO
   * @param mapRedStatsPerStage TODO
   * @param hiveConf TODO
   *
   * @throws Exception TODO
   */
  public static void insertAuditLogEntry(
      AuditLogHook auditLogHook,
      HiveOperation operation,
      String command,
      List<Table> inputTables,
      List<org.apache.hadoop.hive.ql.metadata.Partition> inputPartitions,
      List<Table> outputTables,
      List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions,
      Map<String, MapRedStats> mapRedStatsPerStage,
      HiveConf hiveConf) throws Exception {

    Set<ReadEntity> readEntities = new HashSet<>();
    Set<WriteEntity> writeEntities = new HashSet<>();

    for (Table t : inputTables) {
      readEntities.add(new ReadEntity(t));
    }

    for (org.apache.hadoop.hive.ql.metadata.Partition p : inputPartitions) {
      readEntities.add(new ReadEntity(p));
    }

    for (Table t : outputTables) {
      writeEntities.add(new WriteEntity(t, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    for (org.apache.hadoop.hive.ql.metadata.Partition p : outputPartitions) {
      writeEntities.add(new WriteEntity(p, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    SessionState sessionState = new SessionState(hiveConf);
    sessionState.setCmd(command);
    sessionState.setCommandType(
        operation == null ? null :
            org.apache.hadoop.hive.ql.plan.HiveOperation.valueOf(
                operation.toString()));
    sessionState.setMapRedStats(mapRedStatsPerStage);

    // Run the hook
    auditLogHook.run(sessionState, readEntities, writeEntities, null, null);
  }

  /**
   * Get a hive conf filled with config values.
   *
   * @param mySqlDb the database hive should use to write the audit log to
   * @param dbName the name of the database to be used
   * @param auditCoreLogTableName the table name for the core audit log
   * @param outputObjectsTableName the table name for the output objects
   * @param mapRedStatsTableName the table name for the map-reduce stats
   * @return the hive configuration with the config values set
   */
  public static HiveConf getHiveConf(
      EmbeddedMySqlDb mySqlDb,
      String dbName,
      String auditCoreLogTableName,
      String outputObjectsTableName,
      String mapRedStatsTableName) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(AuditLogHook.JDBC_URL_KEY,
        ReplicationTestUtils.getJdbcUrl(mySqlDb, dbName));
    hiveConf.set(AuditCoreLogModule.TABLE_NAME_KEY, auditCoreLogTableName);
    hiveConf.set(ObjectLogModule.TABLE_NAME_KEY, outputObjectsTableName);
    hiveConf.set(MapRedStatsLogModule.TABLE_NAME_KEY, mapRedStatsTableName);
    return hiveConf;
  }
}
