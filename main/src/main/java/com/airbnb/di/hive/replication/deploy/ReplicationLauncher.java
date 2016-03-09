package com.airbnb.di.hive.replication.deploy;

import com.airbnb.di.db.DbConnectionFactory;
import com.airbnb.di.db.DbConnectionWatchdog;
import com.airbnb.di.db.DbKeyValueStore;
import com.airbnb.di.db.StaticDbConnectionFactory;
import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.PersistedJobInfoStore;
import com.airbnb.di.hive.replication.ReplicationServer;
import com.airbnb.di.hive.replication.auditlog.AuditLogReader;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.ClusterFactory;
import com.airbnb.di.hive.replication.configuration.ConfigurationException;
import com.airbnb.di.hive.replication.configuration.ConfiguredClusterFactory;
import com.airbnb.di.hive.replication.filter.ReplicationFilter;
import com.airbnb.di.hive.replication.thrift.TReplicationService;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

// TODO: Add a handler to exit on uncaught exceptions
public class ReplicationLauncher {
  private static final Log LOG = LogFactory.getLog(
      ReplicationLauncher.class);

  /**
   * Launches the replication sever process using the passed in configuration.
   *
   * @param conf configuration object
   * @param startAfterAuditLogId instruct the server to start replicating entries after this ID
   * @param resetState if there were jobs that were in progress last time the process exited, do not
   *                   resume them and instead mark them as aborted
   *
   * @throws SQLException if there is an error accessing the DB
   * @throws ConfigurationException if there is an error with the supplied configuration
   * @throws IOException if there is an error communicating with services
   */
  public static void launch(Configuration conf,
      Optional<Long> startAfterAuditLogId,
      boolean resetState)
    throws SQLException, ConfigurationException, IOException {

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
    String auditLogMapRedStatsTableName = conf.get(
        DeployConfigurationKeys.AUDIT_LOG_MAP_RED_STATS_DB_TABLE);

    final AuditLogReader auditLogReader = new AuditLogReader(
        auditLogConnectionFactory,
        auditLogTableName,
        auditLogObjectsTableName,
        auditLogMapRedStatsTableName,
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

    final DbKeyValueStore dbKeyValueStore = new DbKeyValueStore(
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

    ClusterFactory clusterFactory = new ConfiguredClusterFactory();
    clusterFactory.setConf(conf);

    final Cluster srcCluster = clusterFactory.getSrcCluster();
    final Cluster destCluster = clusterFactory.getDestCluster();

    String objectFilterClassName = conf.get(
        DeployConfigurationKeys.OBJECT_FILTER_CLASS);
    // Instantiate the class
    Object obj = null;

    try {
      Class<?> clazz = Class.forName(objectFilterClassName);
      obj = clazz.newInstance();
      if (!(obj instanceof ReplicationFilter)) {
        throw new ConfigurationException(String.format(
            "%s is not of type %s",
            obj.getClass().getName(),
            ReplicationFilter.class.getName()));
      }
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new ConfigurationException(e);
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

  /**
   * Launcher entry point.
   *
   * @param argv array of string arguments
   */
  @SuppressWarnings("static-access")
  public static void main(String[] argv)
      throws  SQLException, ConfigurationException, IOException, ParseException {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt("config-files")
        .withDescription("Comma separated list of paths to "
            + "configuration files")
        .hasArg()
        .withArgName("PATH")
        .create());

    options.addOption(OptionBuilder.withLongOpt("start-after-id")
        .withDescription("Start processing entries from the audit "
            + "log after this ID")
        .hasArg()
        .withArgName("ID")
        .create());

    options.addOption(OptionBuilder.withLongOpt("reset-state")
        .create());

    CommandLineParser parser = new BasicParser();
    CommandLine cl = parser.parse(options, argv);

    String configPaths = null;
    Optional<Long> startAfterId = Optional.empty();
    boolean resetState = false;

    if (cl.hasOption("config-files")) {
      configPaths = cl.getOptionValue("config-files");
      LOG.info("configPaths=" + configPaths);
    }

    if (cl.hasOption("start-after-id")) {
      startAfterId = Optional.of(
          Long.parseLong(cl.getOptionValue("start-after-id")));
      LOG.info("startAfterId="  + startAfterId);
    }

    if (cl.hasOption("reset-state")) {
      resetState = true;
      LOG.info("resetState=" + resetState);
    }

    // Require specifying the start ID if resetting the state to make it easier to reason
    if (resetState && startAfterId == null) {
      throw new ConfigurationException("Start after ID must be specified when resetting "
          + "state");
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
      LOG.fatal("Got an exception!", e);
      throw e;
    }
  }
}
