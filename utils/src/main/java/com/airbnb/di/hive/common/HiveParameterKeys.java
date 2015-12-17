package com.airbnb.di.hive.common;

/**
 * Keys used in the parameters map of a Hive Thrift object for storing Airbnb
 * metadata
 */
public class HiveParameterKeys {
    // ID of the last backup - include time information
    public static final String BACKUP_ID = "abb_backup_id";
    // Location of the data for the last backup made for this object
    public static final String BACKUP_DATA_PATH = "abb_backup_data_path";
    // The value of the transient_lastDdl time when the backup occurred. This
    // value is sort of like the last modified time.
    public static final String BACKUP_TLDT = "abb_backup_tldt";
    // If true, updates to this table (including new/updated partitions if
    // applicable) should trigger an automatic backup. Currently unused.
    public static final String BACKUP_ENABLED = "abb_backup_enabled";
    // Indicates the name of the system used to back up
    public static final String BACKUP_SYSTEM = "abb_backup_system";
    // If true, it means that the partition or table is archived to another FS
    public static final String ARCHIVED = "abb_archived";
    // If archived, the original path where the files where stored
    public static final String PRE_ARCHIVE_LOCATION =
            "abb_pre_archive_location";

    public static final String SRC_CLUSTER =
            "abb_source_cluster";

    // Official Hive param keys.
    // TODO: Update with thrift reference
    public static final String TLDT = "transient_lastDdlTime";
    public static final String PROTECT_MODE = "PROTECT_MODE";
}
