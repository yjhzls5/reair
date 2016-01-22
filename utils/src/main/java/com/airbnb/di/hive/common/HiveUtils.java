package com.airbnb.di.hive.common;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveUtils {
  /**
   * TODO.
   *
   * @param table TODO
   * @return true if the given table is partitioned.
   */
  public static boolean isPartitioned(Table table) {
    return table.getPartitionKeys().size() > 0;
  }

  /**
   * TODO.
   *
   * @param table TODO
   * @return true if the given table is a view.
   */
  public static boolean isView(Table table) {
    return TableType.VIRTUAL_VIEW.name().equals(table.getTableType());
  }

  /**
   * TODO.
   *
   * @param ms TODO
   * @param partitionName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public static List<String> partitionNameToValues(HiveMetastoreClient ms, String partitionName)
      throws HiveMetastoreException {
    // Convert the name to a key-value map
    Map<String, String> kv = ms.partitionNameToMap(partitionName);
    List<String> values = new ArrayList<>();

    for (String equalsExpression : partitionName.split("/")) {
      String[] equalsExpressionSplit = equalsExpression.split("=");
      String key = equalsExpressionSplit[0];
      if (!kv.containsKey(key)) {
        // This shouldn't happen, but if it does it implies an error
        // in partition name to map conversion.
        return null;
      }
      values.add(kv.get(key));
    }
    return values;
  }
}
