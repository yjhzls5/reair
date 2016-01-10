package com.airbnb.di.hive.replication;

import com.airbnb.di.hive.common.HiveObjectSpec;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExchangePartitionParser {

    private static String EXCHANGE_REGEX = "\\s*ALTER\\s+TABLE\\s+" +
            "(?<exchangeToTable>\\S+)\\s+EXCHANGE\\s+PARTITION\\s*\\(\\s*" +
            "(?<partitionSpec>.*)\\)\\s+WITH\\s+TABLE\\s+"+
            "(?<exchangeFromTable>\\S+)\\s*";

    private HiveObjectSpec exchangeFromTableSpec;
    private HiveObjectSpec exchangeToTableSpec;
    private String partitionName;
    private List<String> partitionValues;

    public boolean parse(String query) {
        Matcher m = Pattern
                .compile(EXCHANGE_REGEX, Pattern.CASE_INSENSITIVE)
                .matcher(query);

        if (!m.matches()) {
            return false;
        }

        String exchangeFromTable = m.group("exchangeFromTable");
        String exchangeToTable = m.group("exchangeToTable");
        String partitionSpec = m.group("partitionSpec");

        exchangeFromTableSpec = getSpec(exchangeFromTable);
        exchangeToTableSpec = getSpec(exchangeToTable);
        partitionName = getPartitionName(partitionSpec);
        partitionValues = getPartitionValues(partitionSpec);
        return true;
    }

    /**
     *
     * @param spec table specification in the form "db.table"
     * @return
     */
    private HiveObjectSpec getSpec(String spec) {
        String[] specSplit = spec.split("\\.");
        if (specSplit.length == 1) {
            // TODO: Address assumption of default DB
            return new HiveObjectSpec("default", specSplit[0]);
        } else if (specSplit.length == 2) {
            return new HiveObjectSpec(specSplit[0], specSplit[1]);
        } else {
            throw new RuntimeException("Unexpected split from " + spec +
                    " to " + Arrays.asList(specSplit));
        }
    }

    /**
     *
     * param partitionSpec a partition specification in the form "ds=1, hr=2"
     * @return the partition spec converted to that name
     */

    private String getPartitionName(String partitionSpec) {
        // TODO: This is not correct for values that are escaped or with commas
        // There also may be corner cases
        String[] partitionSpecSplit = partitionSpec.split(",");
        StringBuilder sb = new StringBuilder();

        for(String columnSpec : partitionSpecSplit) {
            columnSpec = StringUtils.stripEnd(columnSpec, " \t\n");
            columnSpec = StringUtils.stripStart(columnSpec, " \t\n");
            // columnSpec should be something of the form ds='1'
            String[] columnSpecSplit = columnSpec.split("=");
            if (columnSpecSplit.length != 2) {
                throw new RuntimeException("Unexpected column spec " +
                        columnSpec);
            }

            if (sb.length() != 0) {
                sb.append("/");
            }
            String partitionColumnName = columnSpecSplit[0];
            String partitionColumnValue = columnSpecSplit[1].replace("'", "");
            sb.append(partitionColumnName);
            sb.append("=");
            sb.append(partitionColumnValue);
        }

        return sb.toString();
    }


    private List<String> getPartitionValues(String partitionSpec) {
        // TODO: This is not correct for values that are escaped or with commas
        // There also may be corner cases
        String[] partitionSpecSplit = partitionSpec.split(",");
        List<String> partitionValues = new ArrayList<>();

        for(String columnSpec : partitionSpecSplit) {
            columnSpec = StringUtils.stripEnd(columnSpec, " \t\n");
            columnSpec = StringUtils.stripStart(columnSpec, " \t\n");
            // columnSpec should be something of the form ds='1'
            String[] columnSpecSplit = columnSpec.split("=");
            if (columnSpecSplit.length != 2) {
                throw new RuntimeException("Unexpected column spec " +
                        columnSpec);
            }
            String partitionColumnValue = columnSpecSplit[1].replace("'", "");
            partitionValues.add(partitionColumnValue);
        }
        return partitionValues;
    }


    public HiveObjectSpec getExchangeFromSpec() {
        return exchangeFromTableSpec;
    }

    public HiveObjectSpec getExchangeToSpec() {
        return exchangeToTableSpec;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }
}
