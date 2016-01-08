package com.airbnb.di.hive.replication.deploy;

import com.airbnb.di.hive.replication.auditlog.AuditLogReader;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.db.DbConnectionWatchdog;
import com.airbnb.di.db.DbKeyValueStore;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.configuration.HardCodedCluster;
import com.airbnb.di.hive.replication.PersistedJobInfoStore;
import com.airbnb.di.hive.replication.filter.ReplicationFilter;
import com.airbnb.di.hive.replication.ReplicationServer;
import com.airbnb.di.db.StaticDbConnectionFactory;
import com.airbnb.di.hive.replication.thrift.TReplicationService;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.net.URI;
import java.net.URISyntaxException;

public class ReplicationLauncher {
    private static final Log LOG = LogFactory.getLog(
            ReplicationLauncher.class);

    private static URI makeURI(String thriftUri) throws ConfigurationException {
        try {
            URI uri = new URI(thriftUri);

            if (uri.getPort() <= 0) {
                throw new ConfigurationException("No port specified in " +
                        thriftUri);
            }

            if (!"thrift".equals(uri.getScheme())) {
                throw new ConfigurationException("Not a thrift URI; " +
                        thriftUri);
            }
            return uri;
        } catch (URISyntaxException e) {
            throw new ConfigurationException(e);
        }
    }

    public static void launch(Configuration conf, Long startAfterAuditLogId, boolean resetState)
            throws Exception {

        // Create the audit log reader
        String auditLogJdbcUrl = conf.get(
                DeployConfigurationKeys.AUDIT_LOG_JDBC_URL);
        String auditLogDbUser = conf.get(
                DeployConfigurationKeys.AUDIT_LOG_DB_USER);
        String auditLogDbPassword = conf.get(
                DeployConfigurationKeys.AUDIT_LOG_DB_PASSWORD);
        DbConnectionFactory auditLogConnectionFactory =
                new StaticDbConnectionFactory(
                        auditLogJdbcUrl,
                        auditLogDbUser,
                        auditLogDbPassword);
        String auditLogTableName = conf.get(
                DeployConfigurationKeys.AUDIT_LOG_DB_TABLE);
        String auditLogObjectsTableName = conf.get(
                DeployConfigurationKeys.AUDIT_LOG_OBJECTS_DB_TABLE);

        AuditLogReader auditLogReader = new AuditLogReader(
                auditLogConnectionFactory,
                auditLogTableName,
                auditLogObjectsTableName,
                0);

        // Create the connection to the key value store in the DB
        String stateJdbcUrl = conf.get(
                DeployConfigurationKeys.STATE_JDBC_URL);
        String stateDbUser = conf.get(
                DeployConfigurationKeys.STATE_DB_USER);
        String stateDbPassword = conf.get(
                DeployConfigurationKeys.STATE_DB_PASSWORD);
        String keyValueTableName = conf.get(
                DeployConfigurationKeys.STATE_KV_DB_TABLE);

        DbConnectionFactory stateConnectionFactory =
                new StaticDbConnectionFactory(
                        stateJdbcUrl,
                        stateDbUser,
                        stateDbPassword);

        DbKeyValueStore dbKeyValueStore = new DbKeyValueStore(
                stateConnectionFactory,
                keyValueTableName);

        String stateTableName = conf.get(
                DeployConfigurationKeys.STATE_DB_TABLE);

        // Create the store for replication job info
        PersistedJobInfoStore persistedJobInfoStore =
                new PersistedJobInfoStore(
                        stateConnectionFactory,
                        stateTableName);

        if (resetState) {
            LOG.info("Resetting state by aborting non-completed jobs");
            persistedJobInfoStore.abortRunnableFromDb();
        }

        // Create the source cluster object
        String srcClusterName = conf.get(
                DeployConfigurationKeys.SRC_CLUSTER_NAME);
        String srcMetastoreUrlString = conf.get(
                DeployConfigurationKeys.SRC_CLUSTER_METASTORE_URL);
        URI srcMetastoreUrl = makeURI(srcMetastoreUrlString);
        String srcHdfsRoot = conf.get(
                DeployConfigurationKeys.SRC_HDFS_ROOT);
        String srcHdfsTmp = conf.get(
                DeployConfigurationKeys.SRC_HDFS_TMP);
        Cluster srcCluster = new HardCodedCluster(
                srcClusterName,
                srcMetastoreUrl.getHost(),
                srcMetastoreUrl.getPort(),
                null,
                null,
                new Path(srcHdfsRoot),
                new Path(srcHdfsTmp));

        // Create the dest cluster object
        String destClusterName = conf.get(
                DeployConfigurationKeys.DEST_CLUSTER_NAME);
        String destMetastoreUrlString = conf.get(
                DeployConfigurationKeys.DEST_CLUSTER_METASTORE_URL);
        URI destMetastoreUrl = makeURI(destMetastoreUrlString);
        String destHdfsRoot = conf.get(
                DeployConfigurationKeys.DEST_HDFS_ROOT);
        String destHdfsTmp = conf.get(
                DeployConfigurationKeys.DEST_HDFS_TMP);
        Cluster destCluster = new HardCodedCluster(
                destClusterName,
                destMetastoreUrl.getHost(),
                destMetastoreUrl.getPort(),
                null,
                null,
                new Path(destHdfsRoot),
                new Path(destHdfsTmp));

        String objectFilterClassName = conf.get(
                DeployConfigurationKeys.OBJECT_FILTER_CLASS);
        // Instantiate the class
        Class<?> clazz = Class.forName(objectFilterClassName);
        Object obj = clazz.newInstance();
        if (!(obj instanceof ReplicationFilter)) {
            throw new ConfigurationException(String.format(
                    "%s is not of type %s",
                    obj.getClass().getName(),
                    ReplicationFilter.class.getName()));
        }
        ReplicationFilter filter = (ReplicationFilter) obj;
        filter.setConf(conf);

        int numWorkers = conf.getInt(
                DeployConfigurationKeys.WORKER_THREADS,
                1);

        int maxJobsInMemory = conf.getInt(
                DeployConfigurationKeys.MAX_JOBS_IN_MEMORY,
                100);

        final int thriftServerPort = conf.getInt(
                DeployConfigurationKeys.THRIFT_SERVER_PORT,
                9996);

        LOG.debug("Running replication server");

        ReplicationServer replicationServer = new ReplicationServer(
                conf,
                srcCluster,
                destCluster,
                auditLogReader,
                dbKeyValueStore,
                persistedJobInfoStore,
                filter,
                new DirectoryCopier(conf, destCluster.getTmpDir(), true),
                numWorkers,
                maxJobsInMemory,
                startAfterAuditLogId);

        // Start thrift server
        final TReplicationService.Processor processor =
                new TReplicationService.Processor<TReplicationService.Iface>(
                        replicationServer);

        Runnable serverRunnable = new Runnable() {
            public void run() {
                try {
                    TServerTransport serverTransport = new TServerSocket(
                            thriftServerPort);
                    TServer server = new TSimpleServer(
                            new TServer.Args(
                                    serverTransport).processor(processor));

                    LOG.debug("Starting the thrift server...");
                    server.serve();
                } catch (Exception e) {
                    LOG.error("Thrift server died!", e);
                }
            }
        };

        Thread serverThread = new Thread(serverRunnable);
        serverThread.start();

        // Start DB connection watchdog - kills the server if a DB connection
        // can't be made.
        DbConnectionWatchdog dbConnectionWatchdog = new DbConnectionWatchdog(
                stateConnectionFactory);
        dbConnectionWatchdog.start();

        // Start replicating entries
        try {
            replicationServer.run(Long.MAX_VALUE);
        } finally {
            LOG.debug("Replication server stopped running");
        }
    }

    // Warning suppression needed for the OptionBuilder API
    @SuppressWarnings("static-access")
    public static void main(String[] argv) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withLongOpt("config-files")
                .withDescription("Comma separated list of paths to " +
                        "configuration files")
                .hasArg()
                .withArgName("PATH")
                .create());

        options.addOption(OptionBuilder.withLongOpt("start-after-id")
                .withDescription("Start processing entries from the audit " +
                        "log after this ID")
                .hasArg()
                .withArgName("ID")
                .create());

        options.addOption(OptionBuilder.withLongOpt("reset-state")
                .create());

        CommandLineParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, argv);

        String configPaths = null;
        Long startAfterId = null;
        boolean resetState = false;

        if (cl.hasOption("config-files")) {
            configPaths = cl.getOptionValue("config-files");
            LOG.info("configPaths=" + configPaths);
        }

        if (cl.hasOption("start-after-id")) {
            startAfterId = Long.parseLong(cl.getOptionValue("start-after-id"));
            LOG.info("startAfterId="  + startAfterId);
        }

        if (cl.hasOption("reset-state")) {
            resetState = true;
            LOG.info("resetState=" + resetState);
        }

        // Require specifying the start ID if resetting the state to make it easier to reason
        if (resetState && startAfterId == null) {
            throw new ConfigurationException("Start after ID must be specified when resetting " +
                    "state");
        }

        Configuration conf = new Configuration();

        if (configPaths != null) {
            for (String configPath : configPaths.split(",")) {
                conf.addResource(new Path(configPath));
            }
        }

        try {
            launch(conf, startAfterId, resetState);
        } catch (Exception e) {
            LOG.fatal(e);
            throw e;
        }
    }
}
