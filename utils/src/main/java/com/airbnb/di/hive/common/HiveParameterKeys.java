package com.airbnb.di.hive.common;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;

/**
 * Keys used in the parameters map of a Hive Thrift object for storing Airbnb
 * metadata.
 */
public class HiveParameterKeys {
    public static final String SRC_CLUSTER =
            "abb_source_cluster";

    // Official Hive param keys
    public static final String TLDT = hive_metastoreConstants.DDL_TIME;
}
