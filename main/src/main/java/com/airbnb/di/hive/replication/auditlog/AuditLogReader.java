package com.airbnb.di.hive.replication.auditlog;

import com.airbnb.di.common.Container;
import com.airbnb.di.hive.replication.MetadataException;
import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.hive.hooks.HiveOperation;
import com.airbnb.di.hive.common.NamedPartition;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.utils.RetryableTask;
import com.airbnb.di.utils.RetryingTaskRunner;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Reads entries from the Hive audit log. The ID of the last successfully read
 * ID is persisted.
 */
public class AuditLogReader {

    private static final Log LOG = LogFactory.getLog(AuditLogReader.class);

    private static final int ROW_FETCH_SIZE = 200;

    private DbConnectionFactory dbConnectionFactory;
    private String auditLogTableName;
    private String outputObjectsTableName;
    private long lastReadId;
    private Queue<AuditLogEntry> auditLogEntries;
    private RetryingTaskRunner retryingTaskRunner;

    public AuditLogReader(DbConnectionFactory dbConnectionFactory,
                          String auditLogTableName,
                          String outputObjectsTableName,
                          long getIdsAfter)
            throws SQLException {
        this.dbConnectionFactory = dbConnectionFactory;
        this.auditLogTableName = auditLogTableName;
        this.outputObjectsTableName = outputObjectsTableName;
        this.lastReadId = getIdsAfter;
        auditLogEntries = new LinkedList<>();
        this.retryingTaskRunner = new RetryingTaskRunner();
    }

    public AuditLogReader(DbConnectionFactory dbConnectionFactory,
                          String auditLogTableName,
                          String outputObjectsTableName)
            throws SQLException {
        this.dbConnectionFactory = dbConnectionFactory;
        this.auditLogTableName = auditLogTableName;
        this.outputObjectsTableName = outputObjectsTableName;
        this.lastReadId = -1;
        auditLogEntries = new LinkedList<>();
    }

    public synchronized Optional<AuditLogEntry> resilientNext()
            throws SQLException {
        final Container<Optional<AuditLogEntry>> ret = new Container<>();
        retryingTaskRunner.runUntilSuccessful(new RetryableTask() {
            @Override
            public void run() throws Exception {
                ret.set(next());
            }
        });

        return ret.get();
    }

    public synchronized Optional<AuditLogEntry> next()
            throws SQLException, AuditLogEntryException {
        if (auditLogEntries.size() > 0) {
            return Optional.of(auditLogEntries.remove());
        }

        LOG.debug("Executing queries to try to get more audit log entries " +
                "from the DB");

        fetchMoreEntries();

        if (auditLogEntries.size() > 0) {
            return Optional.of(auditLogEntries.remove());
        } else {
            return Optional.empty();
        }
    }

    /**
     * From the output column in the audit log table, return the partition name.
     * An example is "default.table/ds=1" => "ds=1"
     * @param outputCol
     * @return the partition name
     */
    private String getPartitionNameFromOutputCol(String outputCol) {
        return outputCol.substring(outputCol.indexOf("/") + 1);
    }

    private HiveOperation convertToHiveOperation(String operation) {
        if (operation == null) {
            return null;
        }

        if ("ALTERTABLE_EXCHANGEPARTITION".equals(operation)) {
            return null;
        }

        try {
            return HiveOperation.valueOf(operation);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Given that we start reading after lastReadId and need to get
     * ROW_FETCH_SIZE rows from the audit log, figure out the min and max row
     * ID's to read
     *
     * @throws SQLException
     */
    private LongRange getIdsToRead() throws SQLException {
        String queryFormatString = "SELECT MIN(id) min_id, MAX(id) max_id " +
                "FROM (SELECT id FROM %s WHERE id > %s " +
                "AND (command_type IS NULL OR command_type NOT IN('SHOWTABLES', 'SHOWPARTITIONS', 'SWITCHDATABASE')) " +
                "LIMIT %s)" +
                " subquery";
        String query = String.format(queryFormatString, auditLogTableName,
                lastReadId, ROW_FETCH_SIZE);
        Connection connection = dbConnectionFactory.getConnection();

        PreparedStatement ps = connection.prepareStatement(query);
        LOG.debug("Executing: " + query);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            long minId = rs.getLong("min_id");
            long maxId = rs.getLong("max_id");
            return new LongRange(minId, maxId);
        }
        return new LongRange(0, 0);
    }


    private void fetchMoreEntries() throws SQLException, AuditLogEntryException {

        LongRange idsToRead = getIdsToRead();

        // No more entries to read
        if (idsToRead.getMaximumLong() == 0) {
            return;
        }

        // TODO: Remove left outer join and command type filter once the
        // exchange partition bug is fixed in HIVE-12215
        String queryFormatString = "SELECT a.id, a.create_time, " +
                "command_type, command, name, category, " +
                "type, serialized_object " +
                "FROM %s a LEFT OUTER JOIN %s b on a.id = b.audit_log_id " +
                "WHERE a.id >= %s AND a.id <= %s " +
                "AND (command_type IS NULL OR command_type NOT IN('SHOWTABLES', 'SHOWPARTITIONS', 'SWITCHDATABASE')) " +
                "ORDER BY id";
        String query = String.format(queryFormatString,
                auditLogTableName, outputObjectsTableName,
                idsToRead.getMinimumLong(), idsToRead.getMaximumLong());

        Connection connection = dbConnectionFactory.getConnection();
        PreparedStatement ps = connection.prepareStatement(query);

        ResultSet rs = ps.executeQuery();

        long id = -1;
        Timestamp createTime = null;
        HiveOperation commandType = null;
        String command = null;
        String objectName;
        String objectCategory;
        String objectType;
        String objectSerialized;

        long previouslyReadId = -1;
        Timestamp previouslyReadTs = null;
        HiveOperation previousCommandType = null;
        String previousCommand = null;


        // For a given audit log ID, the join would have produced multiple rows
        // for each ID. Each row contains a single output. Group all the rows
        // and the outputs into a AuditLogEntry.

        // For a given audit log ID, these accumulate the outputs from the
        // different rows.
        List<String> outputDirectories = new LinkedList<>();
        List<Table> outputTables = new LinkedList<>();
        List<NamedPartition> outputPartitions =
                new LinkedList<>();
        List<Table> referenceTables = new LinkedList<>();
        Table renameFromTable = null;
        NamedPartition renameFromPartition = null;

        while(rs.next()) {
            id = rs.getLong("id");
            createTime = rs.getTimestamp("create_time");
            // Invalid operations are returned as null
            String commandTypeString = rs.getString("command_type");
            commandType = convertToHiveOperation(commandTypeString);
            if (commandType == null) {
                LOG.debug(String.format("Invalid operation %s in audit " +
                        "log id: %s", commandTypeString, id));
            }
            command = rs.getString("command");
            objectName = rs.getString("name");
            objectCategory = rs.getString("category");
            objectType = rs.getString("type");
            objectSerialized = rs.getString("serialized_object");

            if (previouslyReadId != -1 && id != previouslyReadId) {
                lastReadId = previouslyReadId;
                // This means that all the outputs for a given audit log entry
                // has been read.
                AuditLogEntry entry = new AuditLogEntry(
                        previouslyReadId,
                        previouslyReadTs,
                        previousCommandType,
                        previousCommand,
                        outputDirectories,
                        referenceTables,
                        outputTables,
                        outputPartitions,
                        renameFromTable,
                        renameFromPartition);
                auditLogEntries.add(entry);
                // Reset these accumulated values
                outputDirectories = new LinkedList<>();
                referenceTables = new LinkedList<>();
                outputTables = new LinkedList<>();
                outputPartitions = new LinkedList<>();
                renameFromPartition = null;
                renameFromTable = null;
            }

            previouslyReadId = id;
            previouslyReadTs = createTime;
            previousCommandType = commandType;
            previousCommand = command;

            if ("DIRECTORY".equals(objectType)) {
                outputDirectories.add(objectName);
            } else if ("TABLE".equals(objectType)) {
                Table t = new Table();
                try {
                    ReplicationUtils.deserializeObject(objectSerialized, t);
                } catch (MetadataException e) {
                    throw new AuditLogEntryException(e);
                }
                if ("OUTPUT".equals(objectCategory)) {
                    outputTables.add(t);
                } else if ("REFERENCE_TABLE".equals(objectCategory)) {
                    referenceTables.add(t);
                } else if ("RENAME_FROM".equals(objectCategory)) {
                    renameFromTable = t;
                } else {
                    throw new RuntimeException("Unhandled category: " +
                            objectCategory);
                }
            } else if ("PARTITION".equals(objectType)) {
                Partition p = new Partition();

                try {
                    ReplicationUtils.deserializeObject(objectSerialized, p);
                } catch (MetadataException e) {
                    throw new AuditLogEntryException(e);
                }
                String partitionName = getPartitionNameFromOutputCol(objectName);
                NamedPartition namedPartition =
                        new NamedPartition(partitionName, p);

                if ("OUTPUT".equals(objectCategory)) {
                    outputPartitions.add(namedPartition);
                } else if ("RENAME_FROM".equals(objectCategory)) {
                    renameFromPartition = namedPartition;
                } else {
                    throw new RuntimeException("Unhandled category: " +
                            objectCategory);
                }
            } else if ("DFS_DIR".equals(objectType)) {
                outputDirectories.add(objectName);
            } else if ("LOCAL_DIR".equals(objectType)) {
                outputDirectories.add(objectName);
            } else if ("DATABASE". equals(objectType)) {
                // Currently, nothing is done with DB's
            } else if (objectType == null) {
                // This will happen for queries that don't have any output
                // objects. This can be removed a long with the OUTER aspect
                // of the join above once the bug with exchange partitions is
                // fixed.
                LOG.debug("No output objects");
            } else {
                throw new RuntimeException("Unhandled output type: " +
                        objectType);
            }
        }

        // This is the case where we read to the end of the table.
        if (id != -1) {
            // TODO: Redundant with lastReadId = idsToRead.getMaximumLong();
            lastReadId = id;
            AuditLogEntry entry = new AuditLogEntry(
                    id,
                    createTime,
                    commandType,
                    command,
                    outputDirectories,
                    referenceTables,
                    outputTables,
                    outputPartitions,
                    renameFromTable,
                    renameFromPartition);
            auditLogEntries.add(entry);
            return;
        }
        // If we constantly get empty results, then this won't be updated. This
        // can happen if the range selected has no objects, but it should be
        lastReadId = idsToRead.getMaximumLong();
    }

    /**
     * Change the reader to start reading entries after this ID
     * @param lastReadId
     */
    public synchronized void setReadAfterId(long lastReadId) {
        this.lastReadId = lastReadId;
        // Clear the audit log entries since it's possible that the reader
        // fetched a bunch of entries in advance, and the ID of those entries
        // may not line up with the new read-after ID.
        auditLogEntries.clear();
    }

    public synchronized Optional<Long> getMaxId() throws SQLException {
        String query = String.format("SELECT MAX(id) FROM %s",
                auditLogTableName);
        Connection connection = dbConnectionFactory.getConnection();
        PreparedStatement ps = connection.prepareStatement(query);

        ResultSet rs = ps.executeQuery();

        rs.next();
        return Optional.ofNullable(rs.getLong(1));
    }
}
