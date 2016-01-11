package com.airbnb.di.hive.replication.deploy;

/**
 * Keys used in the configuration for deploying the replication server
 */
public class DeployConfigurationKeys {
    public static final String AUDIT_LOG_JDBC_URL = "airbnb.reair.audit_log.db.jdbc";
    public static final String AUDIT_LOG_DB_USER = "airbnb.reair.audit_log.db.user";
    public static final String AUDIT_LOG_DB_PASSWORD = "airbnb.reair.audit_log.db.password";
    public static final String AUDIT_LOG_DB_TABLE = "airbnb.reair.audit_log.db.table";
    public static final String AUDIT_LOG_OBJECTS_DB_TABLE = "airbnb.reair.audit_log.objects.db.table";

    public static final String STATE_JDBC_URL = "airbnb.reair.state.db.jdbc";
    public static final String STATE_DB_USER = "airbnb.reair.state.db.user";
    public static final String STATE_DB_PASSWORD = "airbnb.reair.state.db.password";
    public static final String STATE_DB_TABLE = "airbnb.reair.state.db.table";
    public static final String STATE_KV_DB_TABLE = "airbnb.reair.state.kv.db.table";

    public static final String SRC_CLUSTER_NAME = "airbnb.reair.clusters.src.name";
    public static final String SRC_CLUSTER_METASTORE_URL = "airbnb.reair.clusters.src.metastore.url";
    public static final String SRC_HDFS_ROOT = "airbnb.reair.clusters.src.hdfs.root";
    public static final String SRC_HDFS_TMP = "airbnb.reair.clusters.src.hdfs.tmp";

    public static final String DEST_CLUSTER_NAME = "airbnb.reair.clusters.dest.name";
    public static final String DEST_CLUSTER_METASTORE_URL = "airbnb.reair.clusters.dest.metastore.url";
    public static final String DEST_HDFS_ROOT = "airbnb.reair.clusters.dest.hdfs.root";
    public static final String DEST_HDFS_TMP = "airbnb.reair.clusters.dest.hdfs.tmp";

    public static final String BATCH_JOB_OUTPUT_DIR = "airbnb.reair.clusters.batch.output.dir";
    public static final String BATCH_JOB_INPUT_LIST = "airbnb.reair.clusters.batch.input";
    public static final String BATCH_JOB_METASTORE_BLACKLIST = "airbnb.reair.clusters.batch.metastore.blacklist";

    public static final String OBJECT_FILTER_CLASS = "airbnb.reair.object.filter";

    public static final String WORKER_THREADS = "airbnb.reair.worker.threads";
    public static final String MAX_JOBS_IN_MEMORY = "airbnb.reair.jobs.in_memory_count";
    public static final String THRIFT_SERVER_PORT = "airbnb.reair.thrift.port";
}
