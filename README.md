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

To accommodate these use cases, ReAir includes both batch and incremental replication tools. Batch replication executes a one-time copy of a list of tables. Incremental replication is run as a long-running process that copies objects as they are created or changed on the source cluster.

## Additional Documentation

* Blog post
* FAQ

## Batch Replication

### Prerequisites:

* Hadoop (Most, but tested with 2.5.0)
* Hive (Most, but tested with 0.13)

### Run Batch Replication

* Read through and fill out the configuration from the  [template](main/src/main/resources/batch_replication_configuration_template.xml).
* Switch to the repo directory and build the JAR.

```
cd reair
gradlew shadowjar -p main -x test
```

* Create a local text file containing the tables that you want to copy. A row in the text file should consist of the DB name and the table name separated by a tab. e.g.

```
my_db1	my_table1
my_db2	my_table2
```

* Launch the job using the `hadoop jar` command, specifying the config file and the list of tables to copy. A larger heap for the client may be needed for large batches.

```
export HADOOP_HEAPSIZE=8096
hadoop jar airbnb-reair-main-1.0.0-all.jar --config-file my_config_file.xml --table-list my_tables_to_copy.txt
```

* Additional CLI Options: `--step`, `--override-input`. These arguments are useful if want to run one of the three MR job individually. `--step` indicates which step to run. `--override-input` provides the path for the input when running the second and third stage MR jobs. The input path will usually be the output for the first stage MR job.

### Run a Standalone HDFS Copy

* Switch to the repo directory and build the JAR.

```
cd reair
gradlew shadowjar -p main -x test
```

* If HDFS copy logging table does not exist, create using [these commands](main/src/main/resources/create_hdfs_copy_logging_tables.sql)

* CLI options:
```
  -s, --source: source directory
  -d, --destination: destination directory. source and destination must have same relative path.
  -o, --output: logging directory
  -p, --option: checking options: comma separated option including a(add),d(delete),u(update)
  -l, --list: list file size only
  -b, --blacklist: directory name blacklist regex
  -dry, --dryrun: dry run mode
```

* To starts HDFS replication

```
export HADOOP_HEAPSIZE=8096
timestamp="$(date +"%s")"

hadoop jar airbnb-reair-main-1.0.0-all.jar com.airbnb.di.hive.batchreplication.hdfscopy.ReplicationJob -Dmapreduce.job.reduces=500 -Dmapreduce.map.memory.mb=8000 -Dmapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx7000m" -s hdfs://airfs-src/ -d hdfs://airfs-dest/ -o hdfs://airfs-dest/user/test/fullrepljob -b "tmp.*" -p a,u,d

hive -e "LOAD  DATA  INPATH  '/user/test/fullrepljob' OVERWRITE INTO TABLE hdfscopy_result partition ( jobts = $timestamp);"
```

## Incremental Replication

### Prerequisites:

* Hadoop (Most, but tested with 2.5.0)
* Hive (Most, but tested with 0.13)

### Audit Log Hook Setup

Build and deploy the JAR containing the audit log hook

* Switch to the repository directory and build the JAR.


```
cd reair
gradlew shadowjar -p hive-hooks -x test
```

* Once built, the JAR for the audit log hook can be found under `hive-hooks/build/libs/airbnb-reair-hive-hooks-1.0.0-all.jar`

* Copy the JAR to the Hive auxiliary library path. The specifics of the path depending on your setup. Generally, the Hive client's auxiliary library path can be configured using the configuration parameter `hive.aux.jars.path` or through environment variables as defined in shell scripts that launch Hive.

* Create and setup the tables on MySQL required for the audit log. You can create the tables by running the create table commands in all of the .sql files [here](hive-hooks/src/main/resources/). If you're planning to use the same DB to store the tables for incremental replication, also run the create table commands [here](main/src/main/resources/create_tables.sql).

* Configure Hive to use the audit log hook by adding the following sections to `hive-site.xml` from the [audit log configuration template](hive-hooks/src/main/resources/hook_configuration_template.xml). Note: Replace with appropriate values.

* Run a test query and verify that you see the appropriate rows in the `audit_log` and `audit_objects` tables.

### Process Setup

* If the tables for incremental replication were not set up while setting up the audit log,  create the state tables for incremental replication on desired MySQL instance by running the create table commands listed [here](main/src/main/resources/create_tables.sql).

* Read through and fill out the configuration from the [template](main/src/main/resources/replication_configuration_template.xml). You might want to deploy the file to a widely accessible location.

* Switch to the repo directory and build the JAR. You can skip the unit tests if no changes have been made (via the '-x test' flag).

```
cd reair
gradlew shadowjar -p main
```

Once it finishes, the JAR to run the incremental replication process can be found under `main/build/libs/airbnb-reair-main-1.0.0-all.jar`

* To start replicating, add the directory containing the Hive libraries to the Hadoop classpath and then kick off the replication launcher by using the `hadoop jar` command. Be sure to specify the configuration file that was filled out in the prior step.

```
hadoop jar airbnb-reair-main-1.0.0-all.jar com.airbnb.di.hive.replication.deploy.ReplicationLauncher --config-files my_config_file.xml
```

By default logging messages with the `INFO` level will be printed to stderr, but more detailed logging messages with >= `DEBUG` logging level will be recorded to a log file in the current working directory. To see how this is configured, take a look at the [logging configuration file]((main/src/main/resources/log4j.properties).

When the incremental replication process is launched for the first time, it will start replicating entries after the highest numbered ID in the audit log. Because the process periodically checkpoints progress to the DB, it can be killed and will resume from where it left off when restarted. To override this behavior, please see the additional options section.

* Verify that entries are replicated properly by creating a test table on the source cluster and checking to see if it appears on the destination cluster.

* For production deployment, an external process should monitor and restart the replication process if it exits.

### Additional CLI options:

To force the process to start replicating entries after a particular audit log ID, you can pass the `--start-after-id` parameter:

```
hadoop jar airbnb-reair-main-1.0.0-all.jar com.airbnb.di.hive.replication.deploy.ReplicationLauncher --config-files my_config_file.xml --start-after-id 123456
```

Replication entries that were started but not completed on the last invocation will be marked as aborted when you use `--start-after-id` to restart the process.

### Monitoring / Web UI:

The incremental replication process starts a Thrift server that can be used to get metrics and view progress. The Thrift definition is provided [here](thrift/src/main/resources/reair.thrift). A simple web server that displays progress has been included in the `web-server` module. To run the web server:

* Switch to the repo directory and build the JAR's. You can skip the unit tests if no changes have been made.

```
cd reair
gradlew shadowjar -p web-server -x test
```

* The JAR containing the web server can be found at

```
web-server/build/libs/airbnb-reair-web-server-1.0.0-all.jar
```

* Start the web server, specifying the appropriate Thrift host and port where the incremental replication process is running.

```
java -jar airbnb-reair-web-server-1.0.0-all.jar --thrift-host localhost --thrift-port 9996 --http-port 8080
```

* Point your browser to the appropriate URL e.g. `http://localhost:8080` to view the active / retired replication jobs.

### Known Issues
* Due to https://issues.apache.org/jira/browse/HIVE-12865, exchange partition commands will be replicated under limited conditions. Resolution is pending.
* Since the audit log hook writes the changes in a separate transaction from the Hive metastore, it's possible to miss updates if the client fails after the metastore write, but before hook execution. In practice, this is not an issue if failed Hive queries are re-run.
