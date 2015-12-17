package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.batchreplication.metastore.HiveMetastoreClient;
import com.airbnb.di.hive.batchreplication.metastore.HiveMetastoreException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreCompareUtils.CompareTableForReplication;
import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreCompareUtils.UpdateAction.*;

/**
 *
 * A Map/Reduce job that takes in gzipped json log file (like the kind that
 * are dumped out by flog), splits them based on the key data.event_name
 * and writes out the results to gzipped files split on event name.
 */
public class MetastoreReplicationJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MetastoreReplicationJob.class);
    private static final String DRY_RUN_OPTION = "replication.metastore.dryrun";
    private static final String RUN_PARTITION_UPDATE = "replication.metastore.partition.update";
    public static final String METASTORE_BLACKLIST_REGEX = "replication.metastore.blacklist";
    public static final String METASTORE_TO_COPY = "replication.metastore.tocopy";

    private static final ImmutableMap<String, String> HDFSPATH_CHECK_MAP =
            ImmutableMap.of("pinky", "airfs-silver|airfs-pinky|airfs-brain|s3n:|s3a:", "brain", "airfs-brain|s3a:|s3n:");
    private static final ImmutableMap<String, String> HDFSPATH_ROOT_MAP =
            ImmutableMap.of("pinky", "airfs-pinky", "brain", "airfs-brain", "silver", "airfs-silver", "gold", "airfs-gold");

    public static class ProcessTableMapper extends Mapper<Text, Text, LongWritable, Text> {
        private HiveMetastoreClient srcClient;
        private HiveMetastoreClient dstClient;
        private boolean dryrun = true;
        private String metastoreBlackList;
        private String metastore;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String[] parts = context.getConfiguration().get(MetastoreScanInputFormat.SRC_METASTORE_HOST_CONF).split(":");
                this.srcClient = new HiveMetastoreClient(parts[0], Integer.valueOf(parts[1]));
                parts = context.getConfiguration().get(MetastoreScanInputFormat.DST_METASTORE_HOST_CONF).split(":");
                this.dstClient = new HiveMetastoreClient(parts[0], Integer.valueOf(parts[1]));
                this.dryrun = context.getConfiguration().getBoolean(DRY_RUN_OPTION, true);
                this.metastoreBlackList = context.getConfiguration().get(METASTORE_BLACKLIST_REGEX);
                this.metastore = context.getConfiguration().get(METASTORE_TO_COPY, "pinky");
            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        private String genValue(String action, String db, String table, String partition) {
            return Joiner.on("\t").useForNull("").join(action, db, table, partition);
        }

        private List<String> processTable(HiveMetastoreClient srcClient, HiveMetastoreClient dstClient, final String db, final String table)
                throws HiveMetastoreException{
            if (db.matches(metastoreBlackList) || table.matches(metastoreBlackList)) {
                return ImmutableList.of(genValue("skip table", db, table, "source blacklisted"));
            }

            Table tab = srcClient.getTable(db, table);
            Table dstTab = dstClient.getTable(db, table);
            if (tab == null) {
                return ImmutableList.of(genValue("skip table", db, table, "source deleted"));
            }

            if (!tab.getTableType().contains("VIEW")) {
                HdfsPath tablePath = new HdfsPath(tab.getSd().getLocation());
                if (!tablePath.getHost().matches(HDFSPATH_CHECK_MAP.get(metastore))) {
                    return ImmutableList.of(genValue("skip table", db, table, "table is from different cluster"));
                }
                tab.getSd().setLocation(tablePath.replaceHost(HDFSPATH_ROOT_MAP.get("silver")));
            }

            ArrayList<String> ret = new ArrayList<>();

            // if table does not exist on destination
            if (dstTab == null) {
                // If it is not dry run, we create table at destination
                if (!dryrun) {
                    if (dstClient.getDatabase(db) == null) {
                        Database srcDb = srcClient.getDatabase(db);

                        HdfsPath dbPath = new HdfsPath(srcDb.getLocationUri());
                        srcDb.setLocationUri(dbPath.replaceHost(HDFSPATH_ROOT_MAP.get("silver")));
                        dstClient.createDatabase(srcDb);
                    }

                    dstClient.createTable(tab);
                    LOG.info("Create table on destination" + db + ":" + table);
                }

                ret.add(genValue("create table", db, table, tab.getSd().getLocation()));
            } else {
                MetastoreCompareUtils.UpdateAction action = CompareTableForReplication(tab, dstTab);
                switch (action) {
                case NO_CHANGE:
                    if (dryrun) {
                        ret.add(genValue("table meta same", db, table, "table def not changed"));
                    }
                    break;
                case UPDATE:
                    if (!dryrun) {
                        try {
                            dstClient.alterTable(db, table, tab);
                            LOG.info("Update table on destination" + db + ":" + table);
                            ret.add(genValue("table update", db, table, "table def changed"));
                            break;
                        } catch (HiveMetastoreException e) {
                            LOG.info(e.getMessage());
                            LOG.info("retry with receate.");
                        }
                    } else {
                        ret.add(genValue("table update", db, table, "table def changed"));
                        break;
                    }
                case RECREATE:
                    if (!dryrun) {
                        dstClient.dropTable(db, table, false);
                        dstClient.createTable(tab);
                        LOG.info("recreate table on destination" + db + ":" + table);
                    }
                    ret.add(genValue("table recreated", db, table, "table def changed"));
                }
            }

            // for partition tables, need to generate action for each partition change
            if (tab.getPartitionKeys().size() > 0) {
                HashSet<String> partNames = Sets.newHashSet(srcClient.getPartitionNames(db, table));
                HashSet<String> dstPartNames = Sets.newHashSet(dstClient.getPartitionNames(db, table));

                ret.addAll(Lists.transform(Lists.newArrayList(Sets.difference(partNames, dstPartNames)), new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        return genValue("new partition", db, table, s);
                    }
                }));

                ret.addAll(Lists.transform(Lists.newArrayList(Sets.difference(dstPartNames, partNames)), new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        return genValue("delete partition", db, table, s);
                    }
                }));

                ret.addAll(Lists.transform(Lists.newArrayList(Sets.intersection(partNames, dstPartNames)), new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        return genValue("update partition", db, table, s);
                    }
                }));
            }
            return ret;
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                for (String s : processTable(srcClient, dstClient, key.toString(), value.toString())) {
                    context.write(new LongWritable(s.hashCode()), new Text(s));
                }
                LOG.info(String.format("database %s, table %s processed", key.toString(), value.toString()));
            } catch (HiveMetastoreException e) {
                LOG.info(String.format("database %s, table %s got exception", key.toString(), value.toString()));
                LOG.info(e.getMessage());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            srcClient.close();
            dstClient.close();
        }
    }

    public static class CheckPartitionReducer extends Reducer<LongWritable, Text, Text, Text> {
        private HiveMetastoreClient srcClient;
        private HiveMetastoreClient dstClient;
        private int count = 0;
        private boolean dryrun = true;
        private boolean partitionUpdate = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String[] parts = context.getConfiguration().get(MetastoreScanInputFormat.SRC_METASTORE_HOST_CONF).split(":");
                this.srcClient = new HiveMetastoreClient(parts[0], Integer.valueOf(parts[1]));
                parts = context.getConfiguration().get(MetastoreScanInputFormat.DST_METASTORE_HOST_CONF).split(":");
                this.dstClient = new HiveMetastoreClient(parts[0], Integer.valueOf(parts[1]));
                this.dryrun = context.getConfiguration().getBoolean(DRY_RUN_OPTION, true);
                this.partitionUpdate = context.getConfiguration().getBoolean(RUN_PARTITION_UPDATE, false);

            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text value : values) {
                String[] fields = value.toString().split("\t");
                String result = null;
                try {
                    if (fields[0].equals("new partition")) {
                        Partition p = srcClient.getPartition(fields[1], fields[2], fields[3]);
                        result = p != null ? "add partition":"skip add partition";
                        if (!dryrun && p != null) {
                            HdfsPath partitionPath = new HdfsPath(p.getSd().getLocation());
                            if (!partitionPath.getHost().matches(HDFSPATH_CHECK_MAP.get("pinky"))) {
                                result = "paritition is from different cluster";
                            } else {
                                p.getSd().setLocation(partitionPath.replaceHost(HDFSPATH_ROOT_MAP.get("silver")));
                                try {
                                    dstClient.createPartition(p);
                                } catch (HiveMetastoreException e) {
                                    LOG.info("Race condition. Partition already created.");
                                }
                                result = "add Partition";
                            }
                        }
                    } else if (fields[0].equals("update partition")) {
                        if (!dryrun && partitionUpdate) {
                            Partition p = srcClient.getPartition(fields[1], fields[2], fields[3]);
                            Partition p2 = dstClient.getPartition(fields[1], fields[2], fields[3]);
                            if (p !=null && p2 !=null) {
                                HdfsPath dbPath = new HdfsPath(p.getSd().getLocation());
                                p.getSd().setLocation(dbPath.replaceHost(HDFSPATH_ROOT_MAP.get("silver")));

                                MetastoreCompareUtils.UpdateAction action = MetastoreCompareUtils.ComparePartitionForReplication(p, p2);
                                if (action == NO_CHANGE) {
                                    result = "skip update partition";
                                } else if (action == UPDATE) {
                                    dstClient.alterPartition(fields[1], fields[2], p);
                                    result = "update partition";
                                } else if (action == RECREATE) {
                                    dstClient.dropPartition(fields[1], fields[2], fields[3], false);
                                    dstClient.createPartition(p);
                                    result = "recreate partition";
                                }
                            } else {
                                result = "skip update partition";
                            }
                        } else {
                            result = "dry run update partition";
                        }
                    } else if (fields[0].equals("delete partition")) {
                        Partition p = dstClient.getPartition(fields[1], fields[2], fields[3]);
                        result = p != null ? "delete partition":"skip delete partition";
                        if (!dryrun) {
                            try {
                                dstClient.dropPartition(fields[1], fields[2], fields[3], true);
                            }
                            catch (HiveMetastoreException e) {
                                // if delete folder failed, remove without delete folders.
                                dstClient.dropPartition(fields[1], fields[2], fields[3], false);
                            }
                        }
                    } else {
                        result = "no op";
                    }
                } catch (HiveMetastoreException e) {
                    throw new IOException(e);
                }

                context.write(new Text(result), value);
                count++;
                if (count % 100 == 0) {
                    LOG.info("Processed " + count + "partitions");
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            srcClient.close();
            dstClient.close();
        }
    }

    /**
     * Print usage information to provided OutputStream.
     *
     * @param applicationName Name of application to list in usage.
     * @param options Command-line options to be part of usage.
     * @param out OutputStream to which to write the usage information.
     */
    public static void printUsage(
            final String applicationName,
            final Options options,
            final OutputStream out) {
        final PrintWriter writer = new PrintWriter(out);
        final HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printUsage(writer, 80, applicationName, options);
        writer.flush();
    }

    /**
     * Construct and provide GNU-compatible Options.
     *
     * @return Options expected from command-line of GNU form.
     */
    public static Options constructGnuOptions()
    {
        final Options gnuOptions = new Options();
        gnuOptions.addOption("s", "source", true, "source folders").
                addOption("d", "destination", true, "destination folder").
                addOption("o", "output", true, "output folder").
                addOption("ms", "metastore", true, "metastore name").
                addOption("b", "blacklist", true, "folder blacklist regex").
                addOption("update", "partUpdate", false, "check for partition update").
                addOption("dry", "dryrun", false, "dryrun only");
        return gnuOptions;
    }

    public int run(String[] args) throws Exception {
        final CommandLineParser cmdLineGnuParser = new GnuParser();

        final Options gnuOptions = constructGnuOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLineGnuParser.parse(gnuOptions, args);
        }
        catch (ParseException parseException)  // checked exception
        {
            System.err.println(
                    "Encountered exception while parsing using GnuParser:\n"
                            + parseException.getMessage() );
            System.err.println("Usage: hadoop jar ReplicationJob-0.1.jar <in> <out>");
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }

        if (!commandLine.hasOption("s") || !commandLine.hasOption("d")) {
            printUsage("Usage: hadoop jar ReplicationJob-0.1.jar", constructGnuOptions(), System.out);
            return 1;
        }

        if (!commandLine.hasOption("o")) {
            printUsage("Usage: hadoop jar ReplicationJob-0.1.jar", constructGnuOptions(), System.out);
            return 1;
        }

        if (commandLine.hasOption("b")) {
            getConf().set(METASTORE_BLACKLIST_REGEX, commandLine.getOptionValue("b"));
            LOG.info("Blacklist:" + commandLine.getOptionValue("b") );
        }

        if (!commandLine.hasOption("ms")) {
            printUsage("Usage: hadoop jar ReplicationJob-0.1.jar", constructGnuOptions(), System.out);
            return 1;
        }

        return runMetastoreCompareJob(commandLine.getOptionValue("s"), commandLine.getOptionValue("d"),
                commandLine.getOptionValue("o"), commandLine.hasOption("dry"), commandLine.hasOption("update"),
                commandLine.getOptionValue("ms"));
    }

    private int runMetastoreCompareJob(String source, String destination, String output, boolean dryrun, boolean checkPartUpdate, String metastore)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(getConf(), "metastore Compare job");
        job.setJarByClass(getClass());

        job.setInputFormatClass(MetastoreScanInputFormat.class);
        job.setMapperClass(ProcessTableMapper.class);
        job.setReducerClass(CheckPartitionReducer.class);


        //last folder is destination, all other folders are source folder
        job.getConfiguration().set(MetastoreScanInputFormat.SRC_METASTORE_HOST_CONF, source);
        job.getConfiguration().set(MetastoreScanInputFormat.DST_METASTORE_HOST_CONF, destination);
        job.getConfiguration().setBoolean(DRY_RUN_OPTION, dryrun);
        job.getConfiguration().setBoolean(RUN_PARTITION_UPDATE, checkPartUpdate);
        job.getConfiguration().set(METASTORE_TO_COPY, metastore);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MetastoreReplicationJob(), args);
        System.exit(res);
    }
}
