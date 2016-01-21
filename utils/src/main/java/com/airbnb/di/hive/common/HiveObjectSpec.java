package com.airbnb.di.hive.common;

import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Specification for a Hive object (table or partition). Used because having 3 arguments (db, table,
 * partition) for every function gets old.
 */
public class HiveObjectSpec {
  private String dbName = null;

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  private String tableName = null;
  private String partitionName = null;

  public HiveObjectSpec(Table t) {
    this(t.getDbName(), t.getTableName());
  }

  public HiveObjectSpec(NamedPartition p) {
    this(p.getPartition().getDbName(), p.getPartition().getTableName(), p.getName());
  }

  public HiveObjectSpec(String dbName, String tableName) {
    this.dbName = dbName;
    this.tableName = tableName;
  }

  public HiveObjectSpec(String dbName, String tableName, String partitionName) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.partitionName = partitionName;
  }

  public boolean isPartition() {
    return this.partitionName != null;
  }

  public String toString() {
    if (partitionName == null) {
      return String.format("%s.%s", dbName, tableName);
    } else {
      return String.format("%s.%s/%s", dbName, tableName, partitionName);
    }
  }

  public HiveObjectSpec getTableSpec() {
    if (!isPartition()) {
      throw new RuntimeException("Should only be called for " + "partition specs!");
    }

    return new HiveObjectSpec(dbName, tableName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    HiveObjectSpec that = (HiveObjectSpec) o;

    if (!dbName.equals(that.dbName))
      return false;
    if (partitionName != null ? !partitionName.equals(that.partitionName)
        : that.partitionName != null)
      return false;
    if (!tableName.equals(that.tableName))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = dbName.hashCode();
    result = 31 * result + tableName.hashCode();
    result = 31 * result + (partitionName != null ? partitionName.hashCode() : 0);
    return result;
  }
}
