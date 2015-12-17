package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.batchreplication.metastore.HiveMetastoreClient;
import com.airbnb.di.hive.batchreplication.metastore.HiveMetastoreException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.List;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.getClusterName;
import static com.airbnb.di.hive.batchreplication.ReplicationUtils.removeOutputDirectory;


/**
 *
 * A Map/Reduce job that takes in gzipped json log file (like the kind that
 * are dumped out by flog), splits them based on the key data.event_name
 * and writes out the results to gzipped files split on event name.
 */
public class S3ArchiveCleanupJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(S3ArchiveCleanupJob.class);
    private static final String DRY_RUN_OPTION = "replication.metastore.dryrun";

    private static final ImmutableMap<String, String> HDFSPATH_CHECK_MAP =
            ImmutableMap.of("pinky", "airfs-silver|airfs-pinky|airfs-brain|s3n:|s3a:", "brain", "airfs-brain|s3a:|s3n:");
    private static final ImmutableMap<String, String> HDFSPATH_ROOT_MAP =
            ImmutableMap.of("pinky", "airfs-pinky", "brain", "airfs-brain", "silver", "airfs-silver", "gold", "airfs-gold");

    public static class ProcessTableMapper extends Mapper<Text, Text, LongWritable, Text> {
        private HiveMetastoreClient srcClient;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String[] parts = context.getConfiguration().get(MetastoreScanInputFormat.SRC_METASTORE_HOST_CONF).split(":");
                this.srcClient = new HiveMetastoreClient(parts[0], Integer.valueOf(parts[1]));
            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        private String genValue(String action, String db, String table, String partition) {
            return Joiner.on("\t").useForNull("").join(action, db, table, partition);
        }

        private List<String> processTable(HiveMetastoreClient srcClient, final String db, final String table)
                throws HiveMetastoreException{
            Table tab = srcClient.getTable(db, table);
            if (tab == null) {
                return ImmutableList.of(genValue("skip table", db, table, "table deleted"));
            }

            // we can only check for partitioned table. For non-partitioned table if they are archived we can't infer
            // there hdfs path.
            if (tab.getPartitionKeys().size() > 0) {
                return Lists.transform(srcClient.getPartitionNames(db, table), new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        return genValue("check partition", db, table, s);
                    }
                });
            }
            return new ArrayList<>();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                for (String s : processTable(srcClient, key.toString(), value.toString())) {
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
        }
    }

    public static class CheckPartitionReducer extends Reducer<LongWritable, Text, Text, Text> {
        private HiveMetastoreClient srcClient;
        private int count = 0;
        private boolean dryrun = true;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String[] parts = context.getConfiguration().get(MetastoreScanInputFormat.SRC_METASTORE_HOST_CONF).split(":");
                this.srcClient = new HiveMetastoreClient(parts[0], Integer.valueOf(parts[1]));
                this.dryrun = context.getConfiguration().getBoolean(DRY_RUN_OPTION, true);

            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text value : values) {
                String[] fields = value.toString().split("\t");
                String db = fields[1];
                String tbl = fields[2];
                String part = fields[3];
                String result = null;
                try {
                    Table table = srcClient.getTable(db, tbl);
                    Partition p = srcClient.getPartition(db, tbl, part);
                    if (table !=null && p != null) {
                        HdfsPath tablePath = new HdfsPath(table.getSd().getLocation());
                        HdfsPath partitionPath = new HdfsPath(p.getSd().getLocation());
                        if (getClusterName(partitionPath).equals("s3") && !getClusterName(tablePath).equals("s3")) {
                            // for S3 backed partition check hdfs to see if fold still exists
                            String hdfsPartPath = tablePath.getFullPath() + "/" + Joiner.on("/").join(p.getValues());
                            Path file = new Path(hdfsPartPath);
                            FileSystem fs = file.getFileSystem(context.getConfiguration());

                            if (fs.exists(file)) {
                                result = file.toString() + ":" + String.valueOf(fs.getFileStatus(file).getLen());
                                fs.delete(file, true);
                            } else {
                                result = file.toString();
                            }

                            context.write(value, new Text(result));
                        }
                    }
                } catch (HiveMetastoreException e) {
                    throw new IOException(e);
                }
                count++;
                if (count % 100 == 0) {
                    LOG.info("Processed " + count + "partitions");
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            srcClient.close();
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
        gnuOptions.addOption("s", "source", true, "source metastore").
                addOption("o", "output", true, "output folder").
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

        if (!commandLine.hasOption("s")) {
            printUsage("Usage: hadoop jar ReplicationJob-0.1.jar", constructGnuOptions(), System.out);
            return 1;
        }

        if (!commandLine.hasOption("o")) {
            printUsage("Usage: hadoop jar ReplicationJob-0.1.jar", constructGnuOptions(), System.out);
            return 1;
        }

        removeOutputDirectory(commandLine.getOptionValue("o"), this.getConf());

        return runMetastoreCompareJob(commandLine.getOptionValue("s"),
                commandLine.getOptionValue("o"), commandLine.hasOption("dry"));
    }

    private int runMetastoreCompareJob(String source, String output, boolean dryrun)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(getConf(), "metastore Compare job");
        job.setJarByClass(getClass());

        job.setInputFormatClass(MetastoreScanInputFormat.class);
        job.setMapperClass(ProcessTableMapper.class);
        job.setReducerClass(CheckPartitionReducer.class);


        //last folder is destination, all other folders are source folder
        job.getConfiguration().set(MetastoreScanInputFormat.SRC_METASTORE_HOST_CONF, source);
        job.getConfiguration().setBoolean(DRY_RUN_OPTION, dryrun);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new S3ArchiveCleanupJob(), args);
        System.exit(res);
    }
}
