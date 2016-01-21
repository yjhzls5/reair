package test;

import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test for the fake metastore client that we'll use later for testing
 */
public class MockHiveMetastoreClientTest {
  private static MockHiveMetastoreClient mockHiveMetastoreClient;

  @Before
  public void setUp() {
    mockHiveMetastoreClient = new MockHiveMetastoreClient();
  }

  @Test
  public void testCreateAndDropTable() throws HiveMetastoreException {
    String dbName = "test_db";
    String tableName = "test_table";

    // First create a db and a table
    mockHiveMetastoreClient.createDatabase(new Database(dbName, null, null, null));
    Table t = new Table();
    t.setDbName(dbName);
    t.setTableName(tableName);
    mockHiveMetastoreClient.createTable(t);

    // Verify that you get the same table back
    assertEquals(mockHiveMetastoreClient.getTable(dbName, tableName), t);

    // Drop it
    mockHiveMetastoreClient.dropTable(dbName, tableName, false);

    // Verify that you can't get the table any more
    assertNull(mockHiveMetastoreClient.getTable(dbName, tableName));
    assertFalse(mockHiveMetastoreClient.existsTable(dbName, tableName));
  }

  @Test
  public void testCreateAndDropPartition() throws HiveMetastoreException {
    String dbName = "test_db";
    String tableName = "test_table";
    String partitionName = "ds=1/hr=2";
    List<String> partitionValues = new ArrayList<>();
    partitionValues.add("1");
    partitionValues.add("2");

    // First create the db and a partitioned table
    mockHiveMetastoreClient.createDatabase(new Database(dbName, null, null, null));
    Table t = new Table();
    t.setDbName(dbName);
    t.setTableName(tableName);

    List<FieldSchema> partitionCols = new ArrayList<>();
    partitionCols.add(new FieldSchema("ds", "string", "my ds comment"));
    partitionCols.add(new FieldSchema("hr", "string", "my hr comment"));
    t.setPartitionKeys(partitionCols);

    mockHiveMetastoreClient.createTable(t);

    // Then try adding a partition
    Partition p = new Partition();
    p.setDbName(dbName);
    p.setTableName(tableName);
    p.setValues(partitionValues);
    mockHiveMetastoreClient.addPartition(p);

    // Verify that you get back the same partition
    assertEquals(mockHiveMetastoreClient.getPartition(dbName, tableName, partitionName), p);

    // Try dropping the partition and verify that it doesn't exist
    mockHiveMetastoreClient.dropPartition(dbName, tableName, partitionName, false);
    assertNull(mockHiveMetastoreClient.getPartition(dbName, tableName, partitionName));
    assertFalse(mockHiveMetastoreClient.existsPartition(dbName, tableName, partitionName));

    // Try adding a partition again
    mockHiveMetastoreClient.addPartition(p);

    // Drop the table
    mockHiveMetastoreClient.dropTable(dbName, tableName, false);

    // Verify that the partition doesn't exist
    assertNull(mockHiveMetastoreClient.getPartition(dbName, tableName, partitionName));
  }

  @Test
  public void testPartitionNameToMap() throws HiveMetastoreException {
    String partitionName = "ds=1/hr=2/min=3/sec=4";
    LinkedHashMap expectedKeyValueMap = new LinkedHashMap<String, String>();
    expectedKeyValueMap.put("ds", "1");
    expectedKeyValueMap.put("hr", "2");
    expectedKeyValueMap.put("min", "3");
    expectedKeyValueMap.put("sec", "4");

    Map<String, String> keyValueMap = mockHiveMetastoreClient.partitionNameToMap(partitionName);

    // Double check if iteration over the keySet / values is defined
    assertEquals(expectedKeyValueMap.keySet(), keyValueMap.keySet());
    assertEquals(Lists.newArrayList(expectedKeyValueMap.values()),
        Lists.newArrayList(keyValueMap.values()));
  }

  @Test
  public void testRenameTable() throws HiveMetastoreException {
    String dbName = "test_db";
    String tableName = "test_table";
    String newTableName = "new_test_table";

    // First create the DB and the table
    mockHiveMetastoreClient.createDatabase(new Database(dbName, null, null, null));
    Table t = new Table();
    t.setDbName(dbName);
    t.setTableName(tableName);
    mockHiveMetastoreClient.createTable(t);

    // Verify that you get the same table back
    assertEquals(mockHiveMetastoreClient.getTable(dbName, tableName), t);

    // Rename it
    Table newT = new Table(t);
    newT.setTableName(newTableName);
    mockHiveMetastoreClient.alterTable(dbName, tableName, newT);

    // Verify that you can't get the old table any more
    assertNull(mockHiveMetastoreClient.getTable(dbName, tableName));

    // Verify that you can get the new table
    assertEquals(mockHiveMetastoreClient.getTable(dbName, newTableName), newT);
  }
}
