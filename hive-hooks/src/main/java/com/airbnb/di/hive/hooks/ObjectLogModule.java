package com.airbnb.di.hive.hooks;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.Set;

/**
 * A module for logging the Thrift metadata objects associated with a query.
 */
public class ObjectLogModule extends BaseLogModule {

  public static final String TABLE_NAME_KEY =
      "airbnb.logging.audit_log.objects.table_name";

  // The objects table stores serialized forms of the relevant Hive objects
  // for that query.
  //
  // The category describes why the object was logged.
  // OUTPUT - the query modified or altered this object
  // RENAMED_FROM - the query renamed this object into the OUTPUT object
  // REFERENCE_TABLE - when a partition is changed, the table object is
  // logged as well for reference
  public enum ObjectCategory {OUTPUT, RENAME_FROM, REFERENCE_TABLE}

  private final Set<ReadEntity> readEntities;
  private final Set<WriteEntity> writeEntities;

  private final long auditLogId;

  /**
   * TODO.
   *
   * @param connection TODO
   * @param sessionState TODO
   * @param readEntities TODO
   * @param writeEntities TODO
   * @param auditLogId TODO
   *
   * @throws ConfigurationException TODO
   */
  public ObjectLogModule(final Connection connection,
                         final SessionState sessionState,
                         final Set<ReadEntity> readEntities,
                         final Set<WriteEntity> writeEntities,
                         long auditLogId)
           throws ConfigurationException {
    super(connection, TABLE_NAME_KEY, sessionState);
    this.readEntities = readEntities;
    this.writeEntities = writeEntities;
    this.auditLogId = auditLogId;
  }

  /**
   * TODO.
   *
   * @throws Exception TODO
   */
  public void run() throws Exception {
    // Write out the serialized output objects to a separate table
    // in separate statements. Attempting to write all the objects
    // in a single statement can result in MySQL packet size errors.
    // Consider a dynamic partition query that generates 10K
    final String query = String.format("INSERT INTO %s ("
        + "audit_log_id, "
        + "category, "
        + "type, "
        + "name, "
        + "serialized_object) "
        + "VALUES (?, ?, ?, ?, ?)",
        tableName);

    // partitions with Thrift object sizes of 1KB.
    PreparedStatement ps = connection.prepareStatement(query);

    // If a partition is added to a table, then the table
    // technically changed as well. Record this in the output
    // objects table as a REFERENCE_TABLE
    Set<org.apache.hadoop.hive.ql.metadata.Table>
        tableForPartition =
        new HashSet<org.apache.hadoop.hive.ql.metadata.Table>();

    String commandType = sessionState.getCommandType();
    // TODO: ALTERTABLE_EXCHANGEPARTITION is not yet implemented in Hive
    // see https://issues.apache.org/jira/browse/HIVE-11554. Use
    // HiveOperation class once this is in.
    boolean renameTable = "ALTERTABLE_RENAME".equals(commandType);
    boolean renamePartition =
        "ALTERTABLE_RENAMEPART".equals(commandType)
        || "ALTERTABLE_EXCHANGEPARTITION".equals(commandType);
    boolean renameOperation = renameTable || renamePartition;

    // When renaming a table, the read entities contain
    // source table. When renaming a partition, the read entities
    // contain the renamed partition as well as the partition's
    // table. For the partition case, filter out the table in
    // the read entities.
    String renameFromObject = null;
    if (renameOperation) {
      for (ReadEntity entity : readEntities) {
        if (renamePartition && entity.getType() == Entity.Type.TABLE) {
          continue;
        }
        addToObjectsTable(ps, auditLogId,
            ObjectCategory.RENAME_FROM, entity);
        renameFromObject = toIdentifierString(entity);
      }
    }

    for (Entity entity : writeEntities) {
      // For rename operations, the source object is also in the
      // write entities. For example a rename of `old_table` ->
      // `new_table` will have `old_table` in read entities, and
      // `old_table` and `new_table` in write entities. Since
      // `old_table` is written to the table as a RENAMED_FROM
      // entry, we don't also need a OUTPUT entry for `old_table`
      if (renameOperation && toIdentifierString(entity).equals(renameFromObject)) {
        continue;
      }

      // Otherwise add it as an output
      addToObjectsTable(ps, auditLogId,
          ObjectCategory.OUTPUT, entity);

      // Save the table for the partitions as reference objects
      if (entity.getType() == Entity.Type.PARTITION
          || entity.getType() == Entity.Type.DUMMYPARTITION) {
        tableForPartition.add(
            entity.getPartition().getTable());
      }
    }

    for (org.apache.hadoop.hive.ql.metadata.Table t :
        tableForPartition) {
      // Using DDL_NO_LOCK but the value shouldn't matter
      WriteEntity entity = new WriteEntity(t,
          WriteEntity.WriteType.DDL_NO_LOCK);
      addToObjectsTable(ps, auditLogId,
          ObjectCategory.REFERENCE_TABLE, entity);
    }
  }

  /**
   * Insert the given entity into the objects table using the given {@code ps}.
   * @param ps TODO
   * @param auditLogId TODO
   * @param category TODO
   * @param entity TODO
   * @throws Exception TODO
   */
  private static void addToObjectsTable(
                          PreparedStatement ps,
                          long auditLogId,
                          ObjectCategory category,
                          Entity entity) throws Exception {
    int psIndex = 1;
    ps.setLong(psIndex++, auditLogId);
    ps.setString(psIndex++, category.toString());
    ps.setString(psIndex++, entity.getType().toString());
    ps.setString(psIndex++, toIdentifierString(entity));
    ps.setString(psIndex++, toJson(entity));
    ps.executeUpdate();
  }

  /**
   * Convert the given entity into a string that can be used to identify the
   * object in the audit log table.
   *
   * @param entity TODO
   * @return a string representing {@code e}
   * @throws Exception TODO
   */
  private static String toIdentifierString(Entity entity) throws Exception {
    switch (entity.getType()) {
      case DATABASE:
        return entity.getDatabase().getName();
      case TABLE:
        return String.format("%s.%s",
            entity.getTable().getDbName(),
            entity.getTable().getTableName());
      case PARTITION:
      case DUMMYPARTITION:
        return String.format("%s.%s/%s",
            entity.getPartition().getTPartition().getDbName(),
            entity.getPartition().getTPartition().getTableName(),
            entity.getPartition().getName());
      case LOCAL_DIR:
      case DFS_DIR:
        return entity.getLocation().toString();
      default:
        throw new UnhandledTypeExecption("Unhandled type: "
            + entity.getType() + " entity: " + entity);
    }
  }

  /**
   * Converts the object that the entity represents into a JSON string
   * @param entity the entity to convert.
   *
   * @return a JSON representation of {@code e}
   * @throws Exception@
   */
  private static String toJson(Entity entity)
      throws Exception {
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
    switch (entity.getType()) {
      case DATABASE:
        Database db = entity.getDatabase();
        return serializer.toString(db);
      case TABLE:
        Table tableWithLocation = new Table(
            entity.getTable().getTTable());
        URI dataLocation = entity.getLocation();
        tableWithLocation.getSd().setLocation(
            dataLocation == null ? null : dataLocation.toString());
        return serializer.toString(entity.getTable().getTTable());
      case PARTITION:
      case DUMMYPARTITION:
        Partition partitionWithLocation = new Partition(
            entity.getPartition().getTPartition());
        partitionWithLocation.getSd().setLocation(
            entity.getPartition().getDataLocation().toString());
        return serializer.toString(entity.getPartition().getTPartition());
      case LOCAL_DIR:
      case DFS_DIR:
        return entity.getLocation().toString();
      default:
        throw new UnhandledTypeExecption("Unhandled type: "
            + entity.getType() + " entity: " + entity);
    }
  }
}
