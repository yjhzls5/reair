# ReAir

ReAir is a collection of tools for replicating tables and partitions between Hive data warehouses (aka Hive clusters).

## Overview

The replication features in ReAir are useful for the following use cases:

* Migration of a Hive cluster
* Incremental replication between two clusters
* Disaster recovery

When migrating a Hive cluster, ReAir can be used to copy over existing data to the new cluster. Because ReAir copies both data and metadata, datasets are ready to query as soon as the copy completes.

While many organizations start out with a single Hive cluster, they often want better isolation between production and adhoc workloads. Two isolated Hive clusters accommodate this need well, and with two clusters, there is a need to replicate evolving datasets. ReAir can be used to replicate data from one cluster to another and propagate updates incrementally as they occur.

Lastly, ReAir can be used to replicated datasets to a hot-standby cluster for fast failover in disaster recovery scenarios.

To accommodate these use cases, ReAir includes both batch and incremental replication tools. Batch replication executes a one-time copy of a list of tables and partitions. Incremental replication is run as a long-running process that copies objects as they are created or changed on the source cluster. Please see the sections below for details on how to run each tool.

## Batch Replication

## Incremental Replication

Incremental replication is accomplished by recording the changes the source cluster and then copying those changes over to the destination cluster.

Changes on the source cluster are recorded through the use of a post-execute hook in Hive. The post-execute hook allows execution of code after a query has successfully finished. The audit log hook, as included in ReAir, writes the objects that were changed by a successful query into a table on MySQL.

The incremental replication process reads these changes from the audit log and converts them into one of several replication jobs. The replication jobs do a specific action: copy a partition, rename a table, etc. The jobs are run in parallel, but obey some restrictions to ensure that we see the correct behavior on the destination cluster. For example, it's necessary to create a table before copying a partition of that table. The process writes state of these replication jobs into another MySQL table.

Progress checkpointing is done in two ways. The state of each replication job is recorded as they change. In addition, the ID of the audit log entry is recorded as each entry is converted into a replication job. This ensures that the incremental replication process can be restarted at any point with good efficiency. All necessary information is recorded into MySQL tables.

The replication process follows an eventually-consistent consistency model. Updates to the underlying data directories are guaranteed to be atomic, but there is no guarantees on the ordering of updates between objects.

### Prerequisites:

* Hive (version >= 0.12)
* MySQL (version >= 5.1)

### Audit Log Hook Setup

Build and deploy the JAR containing the audit log hook

* Switch to the repository directory and build the JAR. You can skip the unit tests using `-DskipTests` if no changes have been made.


```
cd reair
mvn clean install -DskipTests
```

* Once built, the JAR for the audit log hook can be found under `hive-hooks/target/airbnb-reair-hive-hooks-1.0.0.jar`

* Copy the JAR to the Hive auxiliary library path. The specifics of the path depending on your setup. Generally, the Hive client's auxiliary library path can be configured using the configuration parameter `hive.aux.jars.path` or through environment variables as defined in shell scripts that launch Hive.

* Create and setup the tables on MySQL required for the audit log. You can create the tables by running the create table commands listed [here](hive-hooks/src/main/resources/create_tables.sql). If you're planning to use the same DB to store the tables for incremental replication, also run the create table commands [here](main/src/main/resources/create_tables.sql).

* Configure Hive to use the audit log hook by adding the following sections to `hive-site.xml` from the [audit log configuration template](hive-hooks/src/main/resources/hook_configuration_template.xml). Note: Replace with appropriate values.

* Run a test query and verify that you see the appropriate rows in the `audit_log` and `audit_objects` tables.

### Process Setup

* If the tables for incremental replication were not set up while setting up the audit log,  create the state tables for incremental replication on desired MySQL instance by running the create table commands listed [here](main/src/main/resources/create_tables.sql).

* Read through and fill out the configuration from the [template](main/src/main/resources/replication_configuration_template.xml). You might want to deploy the file to a widely accessible location.

* Switch to the repo directory and build the JAR. You can skip the unit tests if no changes have been made.

```
cd reair
mvn clean install -DskipTests
```

Once it finishes, the JAR to run the incremental replication process can be found under `main/target/airbnb-reair-main-1.0.0.jar`

* To start replicating, add the directory containing the Hive libraries to the Hadoop classpath and then kick off the replication launcher by using the `hadoop jar` command. Be sure to specify the configuration file that was filled out in the prior step.

```
# Example classpath for Cloudera deployments
export HADOOP_CLASSPATH="/mnt/var/opt/CDH/lib/hive/lib/*"
hadoop jar airbnb-reair-main-1.0.0.jar com.airbnb.di.hive.replication.deploy.ReplicationLauncher --config-files my_config_file.xml
```

By default logging messages with the `INFO` level will be printed to stderr, but more detailed logging messages with >= `DEBUG` logging level will be recorded to a log file in the current working directory. To see how this is configured, take a look at the [logging configuration file]((main/src/main/resources/log4j.properties).

When the incremental replication process is launched for the first time, it will start replicating entries after the highest numbered ID in the audit log. Because the process periodically checkpoints progress to the DB, it can be killed and will resume from where it left off when restarted. To override this behavior, please see the additional options section.

* Verify that entries are replicated properly by creating a test table on the source cluster and checking to see if it appears on the destination cluster.

### Additional CLI options:

To force the process to start replicating entries after a particular audit log ID, you can pass the `--start-after-id` parameter:

```
hadoop jar airbnb-reair-main-1.0.0.jar com.airbnb.di.hive.replication.deploy.ReplicationLauncher --start-after-id 123456
```

Replication entries that were started but not completed on the last invocation will be marked as aborted when you use `--start-after-id` to restart the process.

### Monitoring / Web UI:

The incremental replication process starts a Thrift server that can be used to get metrics and view progress. The Thrift definition is provided [here](thrift/src/main/resources/reair.thrift). A simple web server that displays progress has been included in the `web-server` module. To run the web server:

* Switch to the repo directory and build the JAR's. You can skip the unit tests if no changes have been made.

```
cd reair
mvn clean install -DskipTests
```

* The JAR containing the web server can be found at

```
web-server/target/airbnb-reair-web-server-1.0.0.jar
```

* Start the web server, specifying the appropriate Thrift host and port where the incremental replication process is running.

```
java -jar airbnb-reair-web-server-1.0.0.jar --thrift-host localhost --thrift-port 9996 --http-port 8080
```

* Point your browser to the appropriate URL e.g. `http://localhost:8080` to view the active / retired replication jobs.

### Known Issues
* Due to https://issues.apache.org/jira/browse/HIVE-12865, exchange partition commands will be replicated under limited conditions. Resolution is pending.
* Since the audit log hook writes the changes in a separate transaction from the Hive metastore, it's possible to miss updates if the client fails after the metastore write, but before hook execution. In practice, this is not an issue if failed Hive queries are re-run.
