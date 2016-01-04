package com.airbnb.di.hive.hooks;

import com.airbnb.di.utils.RetryableTask;
import com.airbnb.di.utils.RetryingTaskRunner;
import com.airbnb.di.db.DbCredentials;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.json.JSONArray;
import org.json.JSONObject;


import java.net.InetAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A post-execute hook that writes information about successfully executed
 * queries into a MySQL DB. Every successful query generates an entry in an
 * audit log table. In addition, every output object for a successful query
 * generates an entry in the output objects table.
 */
public class AuditLogHook implements PostExecute {

    public static Logger LOG = Logger.getLogger(AuditLogHook.class);

    // The objects table stores serialized forms of the relevant Hive objects
    // for that query.
    //
    // The category describes why the object was logged.
    // OUTPUT - the query modified or altered this object
    // RENAMED_FROM - the query renamed this object into the OUTPUT object
    // REFERENCE_TABLE - when a partition is changed, the table object is
    // logged as well for reference

    public enum ObjectCategory {OUTPUT, RENAME_FROM, REFERENCE_TABLE}

    // Number of attempts to make
    private static int NUM_ATTEMPTS = 10;
    // Will wait BASE_SLEEP * 2 ^ (attempt no.) between attempts
    private static int BASE_SLEEP = 1;

    // Keys for values in hive-site.xml
    public static String JDBC_URL_KEY =
            "airbnb.hive.audit_log.jdbc_url";
    public static String TABLE_NAME_KEY =
            "airbnb.hive.audit_log.table_name";
    public static String OBJECT_TABLE_NAME_KEY =
            "airbnb.hive.audit_log.objects.table_name";
    public static String DB_USERNAME =
            "airbnb.hive.audit_log.db.username";
    public static String DB_PASSWORD =
            "airbnb.hive.audit_log.db.password";

    private DbCredentials dbCreds;

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
        final String auditLogTableName = conf.get(TABLE_NAME_KEY);
        final String objectsTableName = conf.get(OBJECT_TABLE_NAME_KEY);

        if (jdbcUrl == null) {
            throw new ConfigurationException(JDBC_URL_KEY +
                    " is not defined in the conf!");
        }
        if (auditLogTableName == null) {
            throw new ConfigurationException(TABLE_NAME_KEY +
                    " is not defined in the conf!");
        }
        if (objectsTableName == null) {
            throw new ConfigurationException(OBJECT_TABLE_NAME_KEY +
                    " is not defined in the conf!");
        }

        RetryingTaskRunner runner = new RetryingTaskRunner(NUM_ATTEMPTS,
                BASE_SLEEP);

        final String auditLogQuery = String.format("INSERT INTO %s (" +
                "query_id, " +
                "command_type, " +
                "command, " +
                "inputs, " +
                "outputs, " +
                "username, " +
                "ip) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                auditLogTableName);

        final String outputObjQuery = String.format("INSERT INTO %s (" +
                "audit_log_id, " +
                "category, " +
                "type, " +
                "name, " +
                "serialized_object) " +
                "VALUES (?, ?, ?, ?, ?)",
                objectsTableName);

        long startTime = System.currentTimeMillis();
        LOG.debug("Starting insert into audit log");
        runner.runWithRetries(new RetryableTask() {
            @Override
            public void run() throws Exception {
                Connection connection = DriverManager.getConnection(jdbcUrl,
                        dbCreds.getReadWriteUsername(), dbCreds.getReadWritePassword());
                connection.setTransactionIsolation(
                        Connection.TRANSACTION_READ_COMMITTED);

                // Turn off auto commit so that we can ensure that both the
                // audit log entry and the output rows appear at the same time.
                connection.setAutoCommit(false);

                // Write the main audit log entry
                int auditLogPsIndex = 1;
                PreparedStatement auditLogPs =
                        connection.prepareStatement(auditLogQuery,
                                Statement.RETURN_GENERATED_KEYS);
                auditLogPs.setString(auditLogPsIndex++,
                        sessionState.getQueryId());
                auditLogPs.setString(auditLogPsIndex++,
                        sessionState.getCommandType());
                auditLogPs.setString(auditLogPsIndex++,
                        sessionState.getCmd());
                auditLogPs.setString(auditLogPsIndex++,
                        toJson(readEntities, true));
                auditLogPs.setString(auditLogPsIndex++,
                        toJson(writeEntities, true));
                auditLogPs.setString(auditLogPsIndex++,
                        userGroupInformation == null ? null :
                                userGroupInformation.getUserName());
                auditLogPs.setString(auditLogPsIndex++,
                        InetAddress.getLocalHost().getHostAddress());
                auditLogPs.executeUpdate();

                ResultSet rs = auditLogPs.getGeneratedKeys();
                rs.next();
                long auditLogId = rs.getLong(1);

                // Write out the serialized output objects to a separate table
                // in separate statements. Attempting to write all the objects
                // in a single statement can result in MySQL packet size errors.
                // Consider a dynamic partition query that generates 10K
                // partitions with Thrift object sizes of 1KB. 
                PreparedStatement outputObjPs = connection.prepareStatement(
                        outputObjQuery);

                // If a partition is added to a table, then the table
                // technically changed as well. Record this in the output
                // objects table as a REFERENCE_TABLE
                Set<org.apache.hadoop.hive.ql.metadata.Table>
                        tableForPartition =
                        new HashSet<org.apache.hadoop.hive.ql.metadata.Table>();

                String commandType = sessionState.getCommandType();
                // TODO: ALTERTABLE_EXCHANGEPART is not yet implemented in Hive
                // see https://issues.apache.org/jira/browse/HIVE-11554. Use
                // HiveOperation class once this is in.
                boolean renameTable = "ALTERTABLE_RENAME".equals(commandType);
                boolean renamePartition =
                        "ALTERTABLE_RENAMEPART".equals(commandType) ||
                        "ALTERTABLE_EXCHANGEPART".equals(commandType);
                boolean renameOperation = renameTable || renamePartition;

                // When renaming a table, the read entities contain
                // source table. When renaming a partition, the read entities
                // contain the renamed partition as well as the partition's
                // table. For the partition case, filter out the table in
                // the read entities.
                String renameFromObject = null;
                if (renameOperation) {
                    for (ReadEntity e : readEntities) {
                        if (renamePartition &&
                                e.getType() == Entity.Type.TABLE) {
                            continue;
                        }
                        addToObjectsTable(outputObjPs, auditLogId,
                                ObjectCategory.RENAME_FROM, e);
                        renameFromObject = toIdentifierString(e);
                    }
                }

                for (Entity e : writeEntities) {
                    // For rename operations, the source object is also in the
                    // write entities. For example a rename of `old_table` ->
                    // `new_table` will have `old_table` in read entities, and
                    // `old_table` and `new_table` in write entities. Since
                    // `old_table` is written to the table as a RENAMED_FROM
                    // entry, we don't also need a OUTPUT entry for `old_table`
                    if (renameOperation &&
                            toIdentifierString(e).equals(renameFromObject)) {
                        continue;
                    }

                    // Otherwise add it as an output
                    addToObjectsTable(outputObjPs, auditLogId,
                            ObjectCategory.OUTPUT, e);

                    // Save the table for the partitions as reference objects
                    if (e.getType() == Entity.Type.PARTITION ||
                            e.getType() == Entity.Type.DUMMYPARTITION) {
                        tableForPartition.add(
                                e.getPartition().getTable());
                    }
                }

                for (org.apache.hadoop.hive.ql.metadata.Table t :
                        tableForPartition) {
                    // Using DDL_NO_LOCK but the value shouldn't matter
                    WriteEntity e = new WriteEntity(t,
                            WriteEntity.WriteType.DDL_NO_LOCK);
                    addToObjectsTable(outputObjPs, auditLogId,
                            ObjectCategory.REFERENCE_TABLE, e);
                }
                connection.commit();
            }
        });
        LOG.debug(String.format("Inserting into audit log took %d ms",
                System.currentTimeMillis() - startTime));

    }

    /**
     * Insert the given entity into the objects table using the given {@code ps}
     * @param ps
     * @param auditLogId
     * @param category
     * @param e
     * @throws Exception
     */
    private static void addToObjectsTable(PreparedStatement ps,
                                          long auditLogId,
                                          ObjectCategory category,
                                          Entity e)
            throws Exception {
        int outputObjPsIndex = 1;
        ps.setLong(outputObjPsIndex++,
                auditLogId);
        ps.setString(outputObjPsIndex++,
                category.toString());
        ps.setString(outputObjPsIndex++,
                e.getType().toString());
        ps.setString(outputObjPsIndex++,
                toIdentifierString(e));
        ps.setString(outputObjPsIndex++,
                toJson(e));
        ps.executeUpdate();
    }

    /**
     * Convert the given entity into a string that can be used to identify the
     * object in the audit log table.
     *
     * @param e
     * @return a string representing {@code e}
     * @throws Exception
     */
    private static String toIdentifierString(Entity e) throws Exception {
        switch (e.getType()) {
            case DATABASE:
                return e.getDatabase().getName();
            case TABLE:
                return String.format("%s.%s",
                        e.getTable().getDbName(),
                        e.getTable().getTableName());
            case PARTITION:
            case DUMMYPARTITION:
                return String.format("%s.%s/%s",
                        e.getPartition().getTPartition().getDbName(),
                        e.getPartition().getTPartition().getTableName(),
                        e.getPartition().getName());
            case LOCAL_DIR:
            case DFS_DIR:
                return e.getLocation().toString();
            default:
                throw new UnhandledTypeExecption("Unhandled type: " +
                        e.getType() + " entity: " + e);
        }
    }

    /**
     * Converts the object that the entity represents into a JSON string
     * @param e the entity to convert
     *
     * @return a JSON representation of {@code e}
     * @throws Exception@
     */
    private static String toJson(Entity e)
            throws Exception {
        TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
        switch (e.getType()) {
            case DATABASE:
                Database db = e.getDatabase();
                return serializer.toString(db);
            case TABLE:
                Table tableWithLocation = new Table(
                        e.getTable().getTTable());
                URI dataLocation = e.getLocation();
                tableWithLocation.getSd().setLocation(
                        dataLocation == null ? null : dataLocation.toString());
                return serializer.toString(e.getTable().getTTable());
            case PARTITION:
            case DUMMYPARTITION:
                Partition partitionWithLocation = new Partition(
                        e.getPartition().getTPartition());
                partitionWithLocation.getSd().setLocation(
                        e.getPartition().getDataLocation().toString());
                return serializer.toString(e.getPartition().getTPartition());
            case LOCAL_DIR:
            case DFS_DIR:
                return e.getLocation().toString();
            default:
                throw new UnhandledTypeExecption("Unhandled type: " +
                        e.getType() + " entity: " + e);
        }
    }

    /**
     * Converts the entities into a JSON object. Resulting object will look
     * like:
     *
     * {
     *     "tables": [t1, t2...],
     *     "partitions": [p1, p2...],
     *     "dummy_partitions": [p1, p2...],
     *     "local_directories": [d1, d2...],
     *     "dfs_directories": [d1, d2...]
     * }
     *
     * Where t1... and p1... objects are JSON objects that represent the thrift
     * metadata object. If identifierOnly is true, then only a short string
     * representation of the object will be used instead. e.g.
     * "default.my_table" or "default.my_partitioned_table/ds=1"
     *
     * @param entities
     * @param identifierOnly
     * @return
     * @throws Exception
     */
    private static String toJson(Collection<? extends Entity> entities,
                                 boolean identifierOnly)
            throws Exception {

        if (entities == null) {
            return new JSONObject().toString();
        }

        List<Database> databases = new ArrayList<Database>();
        List<Table> tables = new ArrayList<Table>();
        List<Partition> partitions = new ArrayList<Partition>();
        List<Partition> dummyPartitions = new ArrayList<Partition>();
        List<String> localDirectories = new ArrayList<String>();
        List<String> dfsDirectories = new ArrayList<String>();

        Map<Partition, String> partitionNames =
                new HashMap<Partition, String>();

        for (Entity e : entities) {
            switch (e.getType()) {
                case DATABASE:
                    databases.add(e.getDatabase());
                    break;
                case TABLE:
                    tables.add(e.getTable().getTTable());
                    break;
                case PARTITION:
                    partitions.add(e.getPartition().getTPartition());
                    partitionNames.put(e.getPartition().getTPartition(),
                            e.getPartition().getName());
                    break;
                case DUMMYPARTITION:
                    dummyPartitions.add(e.getPartition().getTPartition());
                    partitionNames.put(e.getPartition().getTPartition(),
                            e.getPartition().getName());
                    break;
                case LOCAL_DIR:
                    localDirectories.add(e.getLocation().toString());
                    break;
                case DFS_DIR:
                    dfsDirectories.add(e.getLocation().toString());
                    break;
                case UDF:
                    LOG.info("Skipping logging of UDF type to audit log - displayName: "
                        + e.getUDF().getDisplayName());
                    break;
                default:
                    throw new UnhandledTypeExecption("Unhandled type: " +
                            e.getType() + " entity: " + e);
            }
        }

        TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());

        JSONArray jsonDatabases = new JSONArray();
        JSONArray jsonTables = new JSONArray();
        JSONArray jsonPartitions = new JSONArray();
        JSONArray jsonDummyPartitions = new JSONArray();
        JSONArray jsonLocalDirs = new JSONArray();
        JSONArray jsonDfsDirs = new JSONArray();

        for (Database db : databases) {
          if (identifierOnly) {
            String i = String.format("%s", db.getName());
            jsonDatabases.put(i);
          } else {
            jsonDatabases.put(new JSONObject(serializer.toString(db)));
          }
        }

        for (Table t : tables) {
            if (identifierOnly) {
                String i = String.format("%s.%s", t.getDbName(),
                        t.getTableName());
                jsonTables.put(i);
            } else {
                jsonTables.put(new JSONObject(serializer.toString(t)));
            }
        }

        for (Partition p : partitions) {
            if (identifierOnly) {
                String i = String.format("%s.%s/%s", p.getDbName(),
                        p.getTableName(),
                        partitionNames.get(p));
                jsonPartitions.put(i);
            } else {
                jsonPartitions.put(new JSONObject(serializer.toString(p)));
            }
        }

        for (Partition p : dummyPartitions) {
            if (identifierOnly) {
                String i = String.format("%s.%s/%s", p.getDbName(),
                        p.getTableName(),
                        partitionNames.get(p));
                jsonDummyPartitions.put(i);
            } else {
                jsonDummyPartitions.put(new JSONObject(serializer.toString(p)));
            }
        }

        for (String dir : localDirectories) {
            jsonLocalDirs.put(dir);
        }

        for (String dir : dfsDirectories) {
            jsonDfsDirs.put(dir);
        }

        JSONObject obj = new JSONObject();

        if (jsonDatabases.length() > 0) {
          obj.put("databases", jsonDatabases);
        }

        if (jsonTables.length() > 0) {
            obj.put("tables", jsonTables);
        }

        if (jsonPartitions.length() > 0) {
            obj.put("partitions", jsonPartitions);
        }

        if (jsonDummyPartitions.length() > 0) {
            obj.put("dummy_partitions", jsonDummyPartitions);
        }

        if (jsonLocalDirs.length() > 0) {
            obj.put("local_directories", jsonLocalDirs);
        }

        if (jsonDfsDirs.length() > 0) {
            obj.put("dfs_directories", jsonDfsDirs);
        }

        return obj.toString();
    }
}
