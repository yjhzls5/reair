package com.airbnb.di.hive.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveUtils {
    /**
     *
     * @param t
     * @return true if the given table is partitioned.
     */
    public static boolean isPartitioned(Table t) {
        return t.getPartitionKeys().size() > 0;
    }

    /**
     *
     * @param t
     * @return true if the given table is a view.
     */
    public static boolean isView(Table t) {
        return "VIRTUAL_VIEW".equals(t.getTableType());
    }


    /**
     *
     * @param t the table to modify to be an external table
     */
    public static void makeExternalTable(Table t) {
        // TODO: These should be pulled from the Thrift definition
        t.getParameters().put("EXTERNAL", "TRUE");
        t.setTableType("EXTERNAL_TABLE");
    }

    /**
     * Converts a partition name into a spec used for DDL commands. For example,
     * ds=1/hr=2 -> PARTITION(ds='1', hr='2')
     *
     * @param partitionName
     * @return
     */
    public static String partitionNameToDdlSpec(String partitionName) {

        String[] partitionNameSplit = partitionName.split("/");
        List<String> columnExpressions = new ArrayList<String>();

        for (String columnValue : partitionNameSplit) {
            // TODO: Handle escaping of partition values
            String[] columnValueSplit = columnValue.split("=");
            if (columnValueSplit.length != 2) {
                throw new RuntimeException("Invalid partition name " +
                        partitionName);
            }
            columnExpressions.add(columnValueSplit[0] + "='" +
                    columnValueSplit[1] + "'");
        }
        return "PARTITION(" + StringUtils.join(columnExpressions, ", ") + ")";
    }

    public static List<String> partitionNameToValues(HiveMetastoreClient ms,
                                                      String partitionName)
            throws HiveMetastoreException {
        // Convert the name to a key-value map
        Map<String, String> kv = ms.partitionNameToMap(partitionName);
        List<String> values = new ArrayList<String>();

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
