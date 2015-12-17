package com.airbnb.di.common;

public class ConfigurationKeys {

    // For copying directories
    // FairScheduler MR pool to use for the DistCp jobs
    public static String DISTCP_POOL =
            "airbnb.copy.distcp.pool";
    // When copying directories, skip verification of files / directories that
    // start with this prefix. Main use case is for _distcp_tmp_ directories
    // that seem to persist on S3 for a while after deleting them
    public static String SKIP_CHECK_FILE_PREFIX =
            "airbnb.copy.skipcheck.file.prefix";
    // Number of seconds to wait after a filesystem operation before checking
    // the filesystem status. Mainly for S3 as it can take some time to be
    // consistent.
    public static String FILESYSTEM_CONSISTENCY_WAIT_TIME =
            "airbnb.copy.fs.consistency.wait.time";
    // Whether the destination directory can be deleted if there are files
    // there that aren't in the source
    public static String canDeleteDest =
            "airbnb.copy.destination.delete.enabled";
    // Whether to first copy to a temporary directory before moving to the final
    // destination directory. Useful for copying to HDFS when you want atomic
    // semantics for the destination directory. Does *not* work when copying to
    // S3
    public static String ATOMIC_COPY =
            "airbnb.copy.atomic";
    // Whether to use s3cmd to delete files on S3. Deleting files though s3n
    // seems to have issues.
    public static String S3CMD_DELETES_ENABLED =
            "airbnb.copy.s3cmd.deletes.enabled";
    // If specified, use this configuration file for S3.
    public static String S3CMD_CONFIGURATION_FILE =
            "airbnb.copy.s3cmd.config";
    // Number of times to try the final directory comparison equality check
    // if there is a file not found exception (for S3 consistency issues)
    public static String FILE_NOT_FOUND_EXCEPTION_ATTEMPTS =
            "airbnb.copy.filenotfound.attempts";
    public static String FILE_NOT_FOUND_EXCEPTION_SLEEP_INTERVAL =
            "airbnb.copy.filenotfound.sleep.interval";


    // For backups
    // TODO: Will be documented / used later
    public static String METASTORE_HOST =
            "airbnb.hive.backup.metastore.host";
    public static String METASTORE_PORT =
            "airbnb.hive.backup.metastore.port";
    public static String BACKUP_ROOT =
            "airbnb.hive.backup.root";
    public static String BACKUP_TMP_ROOT =
            "airbnb.hive.backup.tmp.root";
    public static String RESTORE_TMP_ROOT =
            "airbnb.hive.restore.tmp.root";
    public static String RESULT_JDBC_URL =
            "airbnb.hive.backup.result.jdbc.url";
    public static String RESULT_TABLE_NAME =
            "airbnb.hive.backup.result.table";
    public static String SOURCE_CLUSTER =
            "airbnb.hive.backup.cluster";
    public static String HASH_HADOOP_POOL =
            "airbnb.hive.backup.hash.pool";
}
