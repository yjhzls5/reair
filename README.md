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

## Batch Replication

The batch replication mode is typically used to go through the entire data warehouse. Starting the batch replication process is simple: the user runs a shell command that launches a series of map-reduce jobs. The execution contract is to replicate a list of entities, defined as Hive tables, from the source warehouse to the destination warehouse. The batch replication process is efficient when run multiple times in succession - it detects files that match between the source and the destination and only copies the differences. Likewise, metadata is only updated when needed. This 'idempotent' nature ensures that the tool is easy to run and there's no wasted work. Batch replication will handle entity creation, update, and deletion.

There are several challenges for batch replication. First, in a production data warehouse, the sizes of the entities is not even, but the replication latency should not depend on the largest one. For example, common tables can have < 10 partitions, while the largest tables can have over 100,000 partitions. To keep latency in check, it’s necessary to load balance the replication work. Keeping operational load low is also another important challenge. With such a complex operation, detailed logging is a must to figure out and solve errors as they occur. To address this, batch replication needs to record what was copied, what errors occurred, and what was committed in the metastore. For ease of auditing and reporting, these details are written to HDFS.

To solve the load balancing issues, we designed batch replication as a series of map-reduce (MR) jobs. The two most expensive operations in batch replication are metadata updates and files copies, so those steps are often distributed via a shuffle phase. The three jobs generate the list of files to copy, execute the file copy, and lastly, execute the metadata update.

In the first MR job, entity identifiers are read from HDFS and shuffled evenly to reducers. The reducers run various checks on the entities and produce a mapping from entity to the HDFS directory that needs to be copied.  The second MR job goes through the list of directories generated by the first job and creates a list of files in those directories. The file names are shuffled to reducers based on hash of file path and size. Once shuffled, the reducers execute the copy with good load balancing. The third MR job handles the commit logic for the Hive metastore. Since the list of entities was already load balanced in the first MR job, the third MR job can be map-only.  

### Run Batch Replication

* Read through and fill out the configuration from the  [template](main/src/main/resources/batch_replication_configuration_template.xml). You need to configure batch replication related configurations.  
* Switch to the repo directory and build the JAR.
```
cd reair
mvn clean install -DskipTests
```
* If hive batch replication logging table does not exist, create using [here](main/src/main/resources/create_hive_batch_logging_tables.sql)
* To start batch replicating, provide corresponding hive-metastore, libfb303, and libthrift using -libjars and kick off the MetastoreReplicationJob by using the `hadoop jar` command. Be sure to specify the configuration file that was filled out in the prior step. The number of reducers control how fast the job will run. It is decided by your cluster network bandwidth. In Airbnb's data warehouse, we choose 150 and it works well. At the end, each stage result will be inserted into logging table.   

```
export HADOOP_HEAPSIZE=8096
timestamp="$(date +"%s")"

# Example -libjars for Cloudera deployments
hadoop jar airbnb-reair-main-1.0.0.jar com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob -Dmapreduce.framework.name=yarn -Dmapreduce.job.reduces=150 -libjars /mnt/var/opt/CDH/lib/hive/lib/hive-metastore.jar,/mnt/var/opt/CDH/lib/hive/lib/libfb303-0.9.0.jar,/mnt/var/opt/CDH/lib/hive/lib/libthrift-0.9.0-cdh5-2.jar -config-files my_config_file.xml

hive -e "LOAD  DATA  INPATH  '/user/test/hivecopy_output/step1output' OVERWRITE INTO TABLE hivecopy_stage1_result partition ( jobts = $timestamp);"
hive -e "LOAD  DATA  INPATH  '/user/test/hivecopy_output/step2output' OVERWRITE INTO TABLE hivecopy_stage2_result partition ( jobts = $timestamp);"
hive -e "LOAD  DATA  INPATH  '/user/test/hivecopy_output/step3output' OVERWRITE INTO TABLE hivecopy_stage3_result partition ( jobts = $timestamp);"
```

* Additional CLI Options: --step, --override-input. Those two arguments are used when you are debugging the job and want to run one of three MR job individually. --step indicates which step to run. --override-input provide an input path for second and third stage MR jobs. The input path normally will be the output for the first stage MR job.

```
# Example -libjars for Cloudera deployments
hadoop jar airbnb-reair-main-1.0.0.jar com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob -Dmapreduce.framework.name=yarn -Dmapreduce.job.reduces=150 -libjars /mnt/var/opt/CDH/lib/hive/lib/hive-metastore.jar,/mnt/var/opt/CDH/lib/hive/lib/libfb303-0.9.0.jar,/mnt/var/opt/CDH/lib/hive/lib/libthrift-0.9.0-cdh5-2.jar -config-files my_config_file.xml  --config-files my_config_file.xml --step 2 --override-input hdfs:///user/test/hivecopy_output/step1output/
```

## HDFS Copy Job

Outside of the Hive metastore, Airsync also includes a separate tool to do HDFS copies. While tools like `distcp` / `distcp2` are commonly used, we found several issues during testing:
* Performance on a folder with millions of files or the entire warehouse is poor.
* The error rate can be high and `distcp` does not allow custom error-handling.
* Easy-to-analyze logging is missing.

To solve these problems, we developed a series of (2) MR jobs to handle general HDFS copies. The first job builds a list of splits using a heuristic, level-based directory traversal. Once there are enough directories, mappers traverse those folders to generate the full file list for copying. This is done on both source and destination clusters. The file paths are shuffled based on folder path without cluster name to reducers. Reducer will determine whether a copy is necessary. The second MR job reads the list of files to copy and load balances the copy via a shuffle. During our cluster migration, we observed that the new tool has very few failures compared to the other solutions and was only limited by cluster bandwidth.

### Run HDFS Copy

* Switch to the repo directory and build the JAR.
```
cd reair
mvn clean install -DskipTests
```
* If HDFS copy logging table does not exist, create using [here](main/src/main/resources/create_hdfs_copy_logging_tables.sql)
* CLI options:
```
  -s, --source: source folder
  -d, --destination: destination folder. source and destination must have same relative path.
  -o, --output: logging folder
  -p, --option: checking options: comma separated option including a(add),d(delete),u(update)
  -l, --list: list file size only
  -b, --blacklist: folder name blacklist regex
  -dry, --dryrun: dry run mode
```
* To start batch replicating, following the commands below.
```
export HADOOP_HEAPSIZE=8096
timestamp="$(date +"%s")"

hadoop jar airbnb-reair-main-1.0.0.jar com.airbnb.di.hive.batchreplication.hdfscopy.ReplicationJob -Dmapreduce.framework.name=yarn -Dmapreduce.job.reduces=500 -Dmapreduce.map.memory.mb=8000 -Dmapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx7000m" -s hdfs://airfs-src/ -d hdfs://airfs-dest/ -o hdfs://airfs-dest/user/test/fullrepljob -b "tmp.*" -p a,u,d

hive -e "LOAD  DATA  INPATH  '/user/test/fullrepljob' OVERWRITE INTO TABLE hdfscopy_result partition ( jobts = $timestamp);"
```
