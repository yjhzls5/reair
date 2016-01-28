package com.airbnb.di.hive.batchreplication.hivecopy;

import com.google.common.collect.ImmutableList;

import com.airbnb.di.common.FsUtils;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.ReplicationUtils;
import com.airbnb.di.hive.replication.configuration.ConfigurationException;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

public class MetastoreReplicationJob extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MetastoreReplicationJob.class);

  public static final String USAGE_COMMAND_STR = "Usage: hadoop jar <path to jar> "
      + "<class reference> -libjar <path hive-metastore.jar,libthrift.jar,libfb303.jar> options";

  /**
   * TODO.
   *
   * @param estimate TODO
   * @param spec TODO
   * @return TODO
   */
  public static String serializeJobResult(TaskEstimate estimate, HiveObjectSpec spec) {
    return ReplicationUtils.genValue(estimate.getTaskType().name(),
        String.valueOf(estimate.isUpdateMetadata()),
        String.valueOf(estimate.isUpdateData()),
        !estimate.getSrcPath().isPresent() ? null : estimate.getSrcPath().get().toString(),
        !estimate.getDestPath().isPresent() ? null : estimate.getDestPath().get().toString(),
        spec.getDbName(),
        spec.getTableName(),
        spec.getPartitionName());
  }

  /**
   * TODO.
   *
   * @param result TODO
   * @return TODO
   */
  public static Pair<TaskEstimate, HiveObjectSpec> deseralizeJobResult(String result) {
    String [] fields = result.split("\t");
    TaskEstimate estimate = new TaskEstimate(TaskEstimate.TaskType.valueOf(fields[0]),
        Boolean.valueOf(fields[1]),
        Boolean.valueOf(fields[2]),
        fields[3].equals("NULL") ? Optional.empty() : Optional.of(new Path(fields[3])),
        fields[4].equals("NULL") ? Optional.empty() : Optional.of(new Path(fields[4])));

    HiveObjectSpec spec = null;
    if (fields[7].equals("NULL")) {
      spec = new HiveObjectSpec(fields[5], fields[6]);
    } else {
      spec = new HiveObjectSpec(fields[5], fields[6], fields[7]);
    }

    return Pair.of(estimate, spec);
  }

  /**
   * TODO.
   *
   * @param applicationName TODO
   * @param options TODO
   * @param out TODO
   */
  public static void printUsage(String applicationName, Options options, OutputStream out) {
    PrintWriter writer = new PrintWriter(out);
    HelpFormatter usageFormatter = new HelpFormatter();
    usageFormatter.printUsage(writer, 80, applicationName, options);
    writer.flush();
  }

  /**
   * TODO.
   *
   * @param args TODO
   * @return TODO
   *
   * @throws Exception TODO
   */
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt("config-files")
        .withDescription(
            "Comma separated list of paths to configuration files")
        .hasArg()
        .withArgName("PATH")
        .create());

    options.addOption(OptionBuilder.withLongOpt("step")
        .withDescription("Run specific step")
        .hasArg()
        .withArgName("ST")
        .create());

    options.addOption(OptionBuilder.withLongOpt("override-input")
        .withDescription("Input override for step")
        .hasArg()
        .withArgName("OI")
        .create());

    CommandLineParser parser = new BasicParser();
    CommandLine cl = null;

    try {
      cl = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Encountered exception while parsing using GnuParser:\n" + e.getMessage());
      printUsage(USAGE_COMMAND_STR, options, System.out);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }

    String configPaths = null;

    if (cl.hasOption("config-files")) {
      configPaths = cl.getOptionValue("config-files");
      LOG.info("configPaths=" + configPaths);

      // load configure and merge with job conf
      Configuration conf = new Configuration();

      if (configPaths != null) {
        for (String configPath : configPaths.split(",")) {
          conf.addResource(new Path(configPath));
        }
      }

      mergeConfiguration(conf, this.getConf());
    } else {
      LOG.info("Unit test mode, getting configure from caller");
    }


    int step = -1;
    if (cl.hasOption("step")) {
      step = Integer.valueOf(cl.getOptionValue("step"));
    }

    String finalOutput = this.getConf().get(DeployConfigurationKeys.BATCH_JOB_OUTPUT_DIR);
    if (finalOutput == null) {
      System.err.println(
          DeployConfigurationKeys.BATCH_JOB_OUTPUT_DIR + " is required in configuration file.");
      return 1;
    }

    String inputTableList = this.getConf().get(DeployConfigurationKeys.BATCH_JOB_INPUT_LIST);

    Path outputParent = new Path(finalOutput);
    String step1Out = new Path(outputParent, "step1output").toString();
    String step2Out = new Path(outputParent, "step2output").toString();
    String step3Out = new Path(outputParent, "step3output").toString();
    if (step == -1) {
      FsUtils.deleteDirectory(this.getConf(), new Path(step1Out));
      FsUtils.deleteDirectory(this.getConf(), new Path(step2Out));
      FsUtils.deleteDirectory(this.getConf(), new Path(step3Out));
      int result = 0;
      if (inputTableList != null) {
        result = this.runMetastoreCompareJobWithTextInput(inputTableList, step1Out);
      } else {
        result = this.runMetastoreCompareJob(step1Out);
      }
      return
        result == 0 && this.runHdfsCopyJob(step1Out + "/part*", step2Out) == 0
        ? this.runCommitChangeJob(step1Out + "/part*", step3Out) : 1;
    } else {
      switch (step) {
        case 1:
          FsUtils.deleteDirectory(this.getConf(), new Path(step1Out));
          if (inputTableList != null) {
            return this.runMetastoreCompareJobWithTextInput(inputTableList, step1Out);
          } else {
            return this.runMetastoreCompareJob(step1Out);
          }
        case 2:
          FsUtils.deleteDirectory(this.getConf(), new Path(step2Out));
          if (cl.hasOption("override-input")) {
            step1Out = cl.getOptionValue("override-input");
          }

          return this.runHdfsCopyJob(step1Out + "/part*", step2Out);
        case 3:
          FsUtils.deleteDirectory(this.getConf(), new Path(step3Out));
          if (cl.hasOption("override-input")) {
            step1Out = cl.getOptionValue("override-input");
          }

          return this.runCommitChangeJob(step1Out + "/part*", step3Out);
        default:
          LOG.error("Invalid steps specified:" + step);
          return 1;
      }
    }
  }

  private void mergeConfiguration(Configuration inputConfig, Configuration merged) {
    List<String> mergeKeys = ImmutableList.of(DeployConfigurationKeys.SRC_CLUSTER_NAME,
        DeployConfigurationKeys.SRC_CLUSTER_METASTORE_URL,
        DeployConfigurationKeys.SRC_HDFS_ROOT,
        DeployConfigurationKeys.SRC_HDFS_TMP,
        DeployConfigurationKeys.DEST_CLUSTER_NAME,
        DeployConfigurationKeys.DEST_CLUSTER_METASTORE_URL,
        DeployConfigurationKeys.DEST_HDFS_ROOT,
        DeployConfigurationKeys.DEST_HDFS_TMP,
        DeployConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST,
        DeployConfigurationKeys.BATCH_JOB_CLUSTER_FACTORY_CLASS,
        DeployConfigurationKeys.BATCH_JOB_OUTPUT_DIR,
        DeployConfigurationKeys.BATCH_JOB_INPUT_LIST,
        DeployConfigurationKeys.BATCH_JOB_PARALLELISM,
        DeployConfigurationKeys.BATCH_JOB_POOL
        );

    for (String key : mergeKeys) {
      String value = inputConfig.get(key);
      if (key.equals(DeployConfigurationKeys.BATCH_JOB_PARALLELISM)) {
        if (value != null) {
          merged.set("mapreduce.job.maps", value);
          merged.set("mapreduce.job.reduces", value);
        } else {
          merged.set("mapreduce.job.maps", "150");
          merged.set("mapreduce.job.reduces", "150");
        }
      } else {
        if (value != null) {
          merged.set(key, value);
        }
      }

    }
  }

  private int runMetastoreCompareJob(String output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(this.getConf(), "Stage1: Metastore Compare Job");

    job.setJarByClass(this.getClass());
    job.setInputFormatClass(MetastoreScanInputFormat.class);
    job.setMapperClass(Stage1ProcessTableMapper.class);
    job.setReducerClass(PartitionCompareReducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private int runMetastoreCompareJobWithTextInput(String input, String output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(this.getConf(), "Stage1: Metastore Compare Job with Input List");

    job.setJarByClass(this.getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Stage1ProcessTableMapperWithTextInput.class);
    job.setReducerClass(PartitionCompareReducer.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileInputFormat.setMaxInputSplitSize(job,
        this.getConf().getLong("mapreduce.input.fileinputformat.split.maxsize", 60000L));

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private int runHdfsCopyJob(String input, String output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(this.getConf(), "Stage2: HDFS Copy Job");

    job.setJarByClass(this.getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Stage2FolderCopyMapper.class);
    job.setReducerClass(Stage2FolderCopyReducer.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileInputFormat.setMaxInputSplitSize(job,
        this.getConf().getLong("mapreduce.input.fileinputformat.split.maxsize", 60000L));

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private int runCommitChangeJob(String input, String output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(this.getConf(), "Stage3: Commit Change Job");

    job.setJarByClass(this.getClass());

    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Stage3CommitChangeMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileInputFormat.setMaxInputSplitSize(job,
        this.getConf().getLong("mapreduce.input.fileinputformat.split.maxsize", 60000L));

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  /**
   * TODO.
   *
   * @param args TODO
   *
   * @throws Exception TODO
   */
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
        throw new IOException("Invalid configuration", e);
      }
    }

    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        for (String result : worker.processTable(key.toString(), value.toString())) {
          context.write(new LongWritable((long)result.hashCode()), new Text(result));
        }

        LOG.info(
            String.format("database %s, table %s processed", key.toString(), value.toString()));
      } catch (HiveMetastoreException e) {
        throw new IOException(
            String.format(
                "database %s, table %s got exception", key.toString(), value.toString()), e);
      }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      worker.cleanup();
    }
  }

  public static class Stage1ProcessTableMapperWithTextInput
      extends Mapper<LongWritable, Text, LongWritable, Text> {
    private TableCompareWorker worker = new TableCompareWorker();

    protected void setup(Context context) throws IOException, InterruptedException {
      try {
        worker.setup(context);
      } catch (ConfigurationException e) {
        throw new IOException("Invalid configuration", e);
      }
    }

    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        String [] columns = value.toString().split("\\.");
        if (columns.length != 2) {
          LOG.error(String.format("invalid input at line %d: %s", key.get(), value.toString()));
          return;
        }

        for (String result : worker.processTable(columns[0], columns[1])) {
          context.write(new LongWritable((long)result.hashCode()), new Text(result));
        }

        LOG.info(
            String.format("database %s, table %s processed", key.toString(), value.toString()));
      } catch (HiveMetastoreException e) {
        throw new IOException(
            String.format(
                "database %s, table %s got exception", key.toString(), value.toString()), e);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      worker.cleanup();
    }
  }
}
