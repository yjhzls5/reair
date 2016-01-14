package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.deploy.ConfigurationException;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.genValue;
import static com.airbnb.di.hive.batchreplication.ReplicationUtils.removeOutputDirectory;

public class MetastoreReplicationJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MetastoreReplicationJob.class);

    public static final String USAGE_COMMAND_STR = "Usage: hadoop jar <path to jar> " +
            "<class reference> -libjar <path hive-metastore.jar,libthrift.jar,libfb303.jar> options";

    public static String serializeJobResult(TaskEstimate estimate, HiveObjectSpec spec) {
        return genValue(estimate.getTaskType().name(),
                String.valueOf(estimate.isUpdateMetadata()),
                String.valueOf(estimate.isUpdateData()),
                !estimate.getSrcPath().isPresent()?null:estimate.getSrcPath().get().toString(),
                !estimate.getDestPath().isPresent()?null:estimate.getDestPath().get().toString(),
                spec.getDbName(),
                spec.getTableName(),
                spec.getPartitionName());
    }

    public static Pair<TaskEstimate, HiveObjectSpec> deseralizeJobResult(String result) {
        String [] fields = result.split("\t");
        TaskEstimate estimate = new TaskEstimate(TaskEstimate.TaskType.valueOf(fields[0]),
                Boolean.valueOf(fields[1]),
                Boolean.valueOf(fields[2]),
                fields[3].equals("NULL")? Optional.empty():Optional.of(new Path(fields[3])),
                fields[4].equals("NULL")? Optional.empty():Optional.of(new Path(fields[4])));

        HiveObjectSpec spec = null;
        if (fields[7].equals("NULL")) {
            spec = new HiveObjectSpec(fields[5], fields[6]);
        } else {
            spec = new HiveObjectSpec(fields[5], fields[6], fields[7]);
        }

        return Pair.of(estimate, spec);
    }

    public static void printUsage(String applicationName, Options options, OutputStream out) {
        PrintWriter writer = new PrintWriter(out);
        HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printUsage(writer, 80, applicationName, options);
        writer.flush();
    }

    public static Options constructGnuOptions() {
        Options gnuOptions = new Options();
        gnuOptions.addOption("config-files", "config-files", true, "Comma separated list of paths to configuration files")
                  .addOption("st", "step", true, "Run specific step")
                  .addOption("oi", "override-input", true, "input override for step");

        return gnuOptions;
    }

    public int run(String[] args) throws Exception {
        GnuParser cmdLineGnuParser = new GnuParser();
        Options gnuOptions = constructGnuOptions();

        CommandLine commandLine;
        try {
            commandLine = cmdLineGnuParser.parse(gnuOptions, args);
        } catch (ParseException e) {
            System.err.println("Encountered exception while parsing using GnuParser:\n" + e.getMessage());
            printUsage(USAGE_COMMAND_STR, constructGnuOptions(), System.out);
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }

        String configPaths = null;

        if (commandLine.hasOption("config")) {
            configPaths = commandLine.getOptionValue("config");
            LOG.info("configPaths=" + configPaths);
        } else {
            System.err.println("-config option is required.");
            printUsage(USAGE_COMMAND_STR, constructGnuOptions(), System.out);
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }

        Configuration conf = new Configuration();

        if (configPaths != null) {
            for (String configPath : configPaths.split(",")) {
                conf.addResource(new Path(configPath));
            }
        }

        int step = -1;
        if (commandLine.hasOption("st")) {
            step = Integer.valueOf(commandLine.getOptionValue("st"));
        }

        String finalOutput = conf.get(DeployConfigurationKeys.BATCH_JOB_OUTPUT_DIR);
        if (finalOutput == null) {
            System.err.println(DeployConfigurationKeys.BATCH_JOB_INPUT_LIST + " is required in configuration file.");
            return 1;
        }

        String inputTableList = conf.get(DeployConfigurationKeys.BATCH_JOB_INPUT_LIST);

        Path outputParent = new Path(finalOutput);
        String step1Out = outputParent + "/step1output";
        String step2Out = outputParent + "/step2output";
        String step3Out = outputParent + "/step3output";
        if (step == -1) {
            removeOutputDirectory(step1Out, this.getConf());
            removeOutputDirectory(step2Out, this.getConf());
            removeOutputDirectory(step3Out, this.getConf());
            int result = 0;
            if (inputTableList != null) {
                result = this.runMetastoreCompareJobWithTextInput(conf, inputTableList, step1Out);
            } else {
                result = this.runMetastoreCompareJob(conf, step1Out);
            }
            return result == 0 &&
                    this.runHdfsCopyJob(conf, step1Out + "/part*", step2Out) == 0 ? this
                    .runCommitChangeJob(conf, step1Out + "/part*", step3Out) : 1;
        } else {
            switch (step) {
            case 1:
                removeOutputDirectory(step1Out, this.getConf());
                if (inputTableList != null) {
                    return this.runMetastoreCompareJobWithTextInput(conf, inputTableList, step1Out);
                } else {
                    return this.runMetastoreCompareJob(conf, step1Out);
                }
            case 2:
                removeOutputDirectory(step2Out, this.getConf());
                if (commandLine.hasOption("oi")) {
                    step1Out = commandLine.getOptionValue("oi");
                }

                return this.runHdfsCopyJob(conf, step1Out + "/part*", step2Out);
            case 3:
                removeOutputDirectory(step3Out, this.getConf());
                if (commandLine.hasOption("oi")) {
                    step1Out = commandLine.getOptionValue("oi");
                }

                return this.runCommitChangeJob(conf, step1Out + "/part*", step3Out);
            default:
                LOG.error("Invalid steps specified:" + step);
                return 1;
            }
        }
    }

    private void mergeConfiguration (Configuration inputConfig, Configuration merged) {
        merged.set(DeployConfigurationKeys.SRC_CLUSTER_NAME, inputConfig.get(DeployConfigurationKeys.SRC_CLUSTER_NAME));
        merged.set(DeployConfigurationKeys.SRC_CLUSTER_METASTORE_URL, inputConfig.get(DeployConfigurationKeys.SRC_CLUSTER_METASTORE_URL));
        merged.set(DeployConfigurationKeys.SRC_HDFS_ROOT, inputConfig.get(DeployConfigurationKeys.SRC_HDFS_ROOT));
        merged.set(DeployConfigurationKeys.SRC_HDFS_TMP, inputConfig.get(DeployConfigurationKeys.SRC_HDFS_TMP));
        merged.set(DeployConfigurationKeys.DEST_CLUSTER_NAME, inputConfig.get(DeployConfigurationKeys.DEST_CLUSTER_NAME));
        merged.set(DeployConfigurationKeys.DEST_CLUSTER_METASTORE_URL, inputConfig.get(DeployConfigurationKeys.DEST_CLUSTER_METASTORE_URL));
        merged.set(DeployConfigurationKeys.DEST_HDFS_ROOT, inputConfig.get(DeployConfigurationKeys.DEST_HDFS_ROOT));
        merged.set(DeployConfigurationKeys.DEST_HDFS_TMP, inputConfig.get(DeployConfigurationKeys.DEST_HDFS_TMP));

        String blacklist = inputConfig.get(DeployConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST);
        if (blacklist != null) {
            merged.set(DeployConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST, blacklist);
        }
    }

    private int runMetastoreCompareJob(Configuration inputConfig, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "Stage1: Metastore Compare Job");

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(MetastoreScanInputFormat.class);
        job.setMapperClass(Stage1ProcessTableMapper.class);
        job.setReducerClass(PartitionCompareReducer.class);

        mergeConfiguration(inputConfig, job.getConfiguration());

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);

        return success?0:1;
    }

    private int runMetastoreCompareJobWithTextInput(Configuration inputConfig, String input, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "Stage1: Metastore Compare Job with Input List");

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Stage1ProcessTableMapperWithTextInput.class);
        job.setReducerClass(PartitionCompareReducer.class);

        mergeConfiguration(inputConfig, job.getConfiguration());

        FileInputFormat.setInputPaths(job, new Path(input));
        FileInputFormat.setMaxInputSplitSize(job, 6000L);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);

        return success?0:1;
    }

    private int runHdfsCopyJob(Configuration inputConfig, String input, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "Stage2: HDFS Copy Job");

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Stage2FolderCopyMapper.class);
        job.setReducerClass(Stage2FolderCopyReducer.class);

        mergeConfiguration(inputConfig, job.getConfiguration());

        FileInputFormat.setInputPaths(job, new Path(input));
        FileInputFormat.setMaxInputSplitSize(job, 60000L);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);

        return success?0:1;
    }

    private int runCommitChangeJob(Configuration inputConfig, String input, String output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "Stage3: Commit Change Job");

        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Stage3CommitChangeMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        mergeConfiguration(inputConfig, job.getConfiguration());

        FileInputFormat.setInputPaths(job, new Path(input));
        FileInputFormat.setMaxInputSplitSize(job, 60000L);

        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean success = job.waitForCompletion(true);

        return success?0:1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MetastoreReplicationJob(), args);

        System.exit(res);
    }

    public static class Stage1ProcessTableMapper extends Mapper<Text, Text, LongWritable, Text> {
        private TableCompareWorker worker = new TableCompareWorker();

        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                worker.setup(context);
            } catch (ConfigurationException e) {
                LOG.error("Invalid configuration", e);
                throw new IOException(e.getMessage());
            }
        }

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                for (String result : worker.processTable(key.toString(), value.toString())) {
                    context.write(new LongWritable((long)result.hashCode()), new Text(result));
                }

                LOG.info(String.format("database %s, table %s processed", key.toString(), value.toString()));
            } catch (HiveMetastoreException e) {
                LOG.error(String.format("database %s, table %s got exception", key.toString(), value.toString()), e);
                throw new IOException(e.getMessage());
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            worker.cleanup();
        }
    }

    public static class Stage1ProcessTableMapperWithTextInput extends Mapper<LongWritable, Text, LongWritable, Text> {
        private TableCompareWorker worker = new TableCompareWorker();

        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                worker.setup(context);
            } catch (ConfigurationException e) {
                LOG.error("Invalid configuration", e);
                throw new IOException(e.getMessage());            }
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String [] columns = value.toString().split("\\.");
                if (columns.length != 2) {
                    LOG.error(String.format("invalid input at line %d: %s", key.get(), value.toString()));
                    return;
                }

                for (String result : worker.processTable(columns[0], columns[1])) {
                    context.write(new LongWritable((long)result.hashCode()), new Text(result));
                }

                LOG.info(String.format("database %s, table %s processed", key.toString(), value.toString()));
            } catch (HiveMetastoreException e) {
                LOG.error(String.format("database %s, table %s got exception", key.toString(), value.toString()), e);
                throw new IOException(e.getMessage());
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            worker.cleanup();
        }
    }
}
