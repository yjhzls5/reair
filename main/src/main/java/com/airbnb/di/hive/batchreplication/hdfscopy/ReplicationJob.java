package com.airbnb.di.hive.batchreplication.hdfscopy;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import com.airbnb.di.hive.batchreplication.SimpleFileStatus;
import com.airbnb.di.hive.replication.ReplicationUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * A Map/Reduce job copy hdfs files from source directories, merge the source directories,
 * and copy to destination. In case of conflict in sources, the source with largest
 * timestamp value is picked.
 */
public class ReplicationJob extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ReplicationJob.class);
  private static final String SRC_PATH_CONF = "replication.src.path";
  private static final String DST_PATH_CONF = "replication.dst.path";
  private static final String COMPARE_OPTION_CONF = "replication.compare.option";
  public static final String DIRECTORY_BLACKLIST_REGEX = "replication.directory.blacklist";

  private enum Operation {
    ADD,
    DELETE,
    UPDATE;

    public static Operation getEnum(String value) {
      if (value.equals("a")) {
        return ADD;
      } else if (value.equals("d")) {
        return DELETE;
      } else if (value.equals("u")) {
        return UPDATE;
      }

      throw new RuntimeException("Invalid Operation");
    }
  }

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private static URI findRootUri(URI [] rootUris, Path path) {
    return Stream.of(rootUris).filter(uri -> !uri.relativize(path.toUri()).equals(path.toUri()))
            .findFirst().get();
  }

  public static class ListFileMapper extends Mapper<Text, Boolean, Text, FileStatus> {
    private String directoryBlackList;
    // Store root URI for sources and destination directory
    private URI [] rootUris;

    private void enumDirectories(FileSystem fs, URI rootUri, Path directory, boolean recursive,
        Mapper.Context context) throws IOException, InterruptedException {
      try {
        for (FileStatus status : fs.listStatus(directory, hiddenFileFilter)) {
          if (status.isDirectory()) {
            if (recursive) {
              if (directoryBlackList == null
                  || !status.getPath().getName().matches(directoryBlackList)) {
                enumDirectories(fs,rootUri, status.getPath(), recursive, context);
              }
            }
          } else {
            context.write(new Text(rootUri.relativize(directory.toUri()).getPath()),
                    new FileStatus(status));
          }
        }
        context.progress();
      } catch (FileNotFoundException e) {
        return;
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.rootUris = Stream.concat(
          Stream.of(context.getConfiguration().get(DST_PATH_CONF)),
          Stream.of(context.getConfiguration().get(SRC_PATH_CONF).split(","))).map(
            root -> new Path(root).toUri()).toArray(size -> new URI[size]
          );
      this.directoryBlackList = context.getConfiguration().get(DIRECTORY_BLACKLIST_REGEX);
    }

    @Override
    protected void map(Text key, Boolean value, Context context)
        throws IOException, InterruptedException {
      Path directory = new Path(key.toString());
      FileSystem fileSystem = directory.getFileSystem(context.getConfiguration());

      enumDirectories(fileSystem, findRootUri(rootUris, directory), directory, value, context);
      LOG.info(key.toString() + " processed.");
    }
  }

  private static Text generateValue(String action, SimpleFileStatus fileStatus) {
    ArrayList<String> fields = new ArrayList<>();

    fields.add(action);
    fields.add(fileStatus.getFullPath());
    fields.add(String.valueOf(fileStatus.getFileSize()));
    fields.add(String.valueOf(fileStatus.getModificationTime()));

    return new Text(Joiner.on("\t").useForNull("\\N").join(fields));
  }

  /**
   * Compare source1 + source2 with destination.
   */
  public static class DirectoryCompareReducer extends Reducer<Text, FileStatus, Text, Text> {
    private URI dstRoot;
    // Store root URI for sources and destination directory
    private URI [] rootUris;
    private Predicate<SimpleFileStatus> underDstRootPred;
    private EnumSet<Operation> operationSet;

    private SimpleFileStatus findSrcFileStatus(List<SimpleFileStatus> fileStatuses) {
      // pick copy source. The source is the one with largest timestamp value
      return Ordering.from(new Comparator<SimpleFileStatus>() {
        @Override
        public int compare(SimpleFileStatus o1, SimpleFileStatus o2) {
          return Long.compare(o1.getModificationTime(), o2.getModificationTime());
        }
      }).max(fileStatuses);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      this.dstRoot = new Path(context.getConfiguration().get(DST_PATH_CONF)).toUri();
      this.underDstRootPred = new Predicate<SimpleFileStatus>() {
        @Override
        public boolean apply(@Nullable SimpleFileStatus simpleFileStatus) {
          return !dstRoot.relativize(simpleFileStatus.getUri())
                  .equals(simpleFileStatus.getUri());
        }
      };

      this.operationSet = Sets.newEnumSet(
        Iterables.<String, Operation>transform(
          Arrays.asList(context.getConfiguration().get(COMPARE_OPTION_CONF, "a,d,u")
                  .split(",")),
          new Function<String, Operation>() {
            @Override
            public Operation apply(@Nullable String value) {
              return Operation.getEnum(value);
            }
          }),
        Operation.class);
      this.rootUris = Stream.concat(
              Stream.of(context.getConfiguration().get(DST_PATH_CONF)),
              Stream.of(context.getConfiguration().get(SRC_PATH_CONF).split(","))).map(
                  root -> new Path(root).toUri()).toArray(size -> new URI[size]
      );
    }

    @Override
    protected void reduce(Text key, Iterable<FileStatus> values, Context context)
        throws IOException, InterruptedException {
      ListMultimap<String, SimpleFileStatus> fileStatusHashMap = LinkedListMultimap.create();

      for (FileStatus fs : values) {
        SimpleFileStatus efs =
            new SimpleFileStatus(fs.getPath(), fs.getLen(), fs.getModificationTime());
        URI rootUris = findRootUri(this.rootUris, fs.getPath());
        fileStatusHashMap.put(rootUris.relativize(fs.getPath().toUri()).getPath(), efs);
      }

      for (String relativePath : fileStatusHashMap.keySet()) {
        List<SimpleFileStatus> fileStatuses = fileStatusHashMap.get(relativePath);
        ArrayList<SimpleFileStatus> srcFileStatus =
            Lists.newArrayList(Iterables.filter(fileStatuses,
                    Predicates.not(this.underDstRootPred)));
        ArrayList<SimpleFileStatus> dstFileStatus =
            Lists.newArrayList(Iterables.filter(fileStatuses, this.underDstRootPred));

        // If destination has file,
        if (dstFileStatus.size() > 0) {
          // we can only have one destination
          assert dstFileStatus.size() == 1;

          // There are two cases:
          //     update or delete.
          if (srcFileStatus.size() > 0) {
            // pick source first. The source is the one with largest timestamp value
            SimpleFileStatus finalSrcFileStatus = findSrcFileStatus(srcFileStatus);

            // If file size is different we need to copy
            if (finalSrcFileStatus.getFileSize() != dstFileStatus.get(0).getFileSize()) {
              if (operationSet.contains(Operation.UPDATE)) {
                context.write(new Text(relativePath),
                        generateValue(Operation.UPDATE.toString(), finalSrcFileStatus));
              }
            }
          } else {
            // source does not exist, then we need to delete if operation contains delete.
            if (operationSet.contains(Operation.DELETE)) {
              // 2. source does not exist it is delete
              context.write(new Text(relativePath),
                      generateValue(Operation.DELETE.toString(), dstFileStatus.get(0)));
            }
          }
        } else {
          // Destination does not exist. So we need to add the file if needed.
          if (operationSet.contains(Operation.ADD)) {
            // if no destination, then this is a new file.
            SimpleFileStatus src = findSrcFileStatus(srcFileStatus);
            context.write(new Text(relativePath),
                    generateValue(Operation.ADD.toString(), src));
          }
        }
      }
    }
  }

  // Mapper to rebalance files need to be copied.
  public static class HdfsSyncMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split("\t");
      long hashValue = Hashing.murmur3_128()
          .hashLong(Long.valueOf(fields[3]).hashCode() * Long.valueOf(fields[4]).hashCode())
          .asLong();
      context.write(new LongWritable(hashValue), value);
    }
  }

  public static class HdfsSyncReducer extends Reducer<LongWritable, Text, Text, Text> {
    private String dstRoot;
    private long copiedSize = 0;

    enum CopyStatus {
      COPIED,
      SKIPPED
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      this.dstRoot = context.getConfiguration().get(DST_PATH_CONF);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        String[] fields = value.toString().split("\t");
        Operation operation = Operation.valueOf(fields[1]);

        // We only support add operation for now.
        if (operation == Operation.ADD || operation == Operation.UPDATE) {
          SimpleFileStatus fileStatus =
              new SimpleFileStatus(fields[2], Long.valueOf(fields[3]), Long.valueOf(fields[4]));
          Path dstFile = new Path(dstRoot, fields[0]);

          FileSystem srcFs = (new Path(fileStatus.getFullPath()))
                  .getFileSystem(context.getConfiguration());
          FileSystem dstFs = dstFile.getFileSystem(context.getConfiguration());
          String copyError =
              ReplicationUtils.doCopyFileAction(context.getConfiguration(), fileStatus,
                  srcFs, dstFile.getParent().toString(),
                  dstFs, context, fields[1].equals("update"), "");

          if (copyError == null) {
            context.write(new Text(fields[0]),
                    generateValue(CopyStatus.COPIED.toString(), fileStatus));
          } else {
            context.write(new Text(fields[0]),
                    generateValue(CopyStatus.SKIPPED.toString(), fileStatus));
          }
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      LOG.info("Total bytes copied = " + copiedSize);
    }
  }

  /**
   * Print usage information to provided OutputStream.
   *
   * @param applicationName Name of application to list in usage.
   * @param options Command-line options to be part of usage.
   * @param out OutputStream to which to write the usage information.
   */
  public static void printUsage(final String applicationName, final Options options,
      final OutputStream out) {
    final PrintWriter writer = new PrintWriter(out);
    final HelpFormatter usageFormatter = new HelpFormatter();
    usageFormatter.printUsage(writer, 80, applicationName, options);
    writer.flush();
  }

  /**
   * Construct and provide Options.
   *
   * @return Options expected from command-line of GNU form.
   */
  public static Options constructOptions() {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt("source")
            .withDescription(
                    "Comma separated list of source directories")
            .hasArg()
            .withArgName("S")
            .create());

    options.addOption(OptionBuilder.withLongOpt("destination")
            .withDescription("Copy desitnation directory")
            .hasArg()
            .withArgName("D")
            .create());

    options.addOption(OptionBuilder.withLongOpt("output-path")
            .withDescription("Job logging output path")
            .hasArg()
            .withArgName("O")
            .create());

    options.addOption(OptionBuilder.withLongOpt("operation")
            .withDescription("checking options: comma separated option"
                    + " including a(add), d(delete), u(update)")
            .hasArg()
            .withArgName("P")
            .create());

    options.addOption(OptionBuilder.withLongOpt("blacklist")
            .withDescription("Directory blacklist regex")
            .hasArg()
            .withArgName("B")
            .create());

    options.addOption(OptionBuilder.withLongOpt("dry-run")
            .withDescription("Dry run only")
            .create());

    return options;
  }

  /**
   * Method to run HDFS copy job.
   *  1. Parse program args.
   *  2. Run two MR jobs in sequence.
   *
   * @param args program arguments
   * @return 1 failed
   *         0 succeeded
   *
   * @throws Exception IOException, InterruptedException, ClassNotFoundException
   */
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    final CommandLineParser cmdLineParser = new BasicParser();

    final Options options = constructOptions();
    CommandLine commandLine;

    try {
      commandLine = cmdLineParser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Encountered exception while parsing using GnuParser:", e);
      printUsage("Usage: hadoop jar ...", options, System.out);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
      return 1;
    }


    if (!commandLine.hasOption("source") || !commandLine.hasOption("destination")) {
      printUsage("Usage: hadoop jar ...", constructOptions(), System.out);
      return 1;
    }

    if (!commandLine.hasOption("output-path")) {
      printUsage("Usage: hadoop jar ...", constructOptions(), System.out);
      return 1;
    }

    if (commandLine.hasOption("blacklist")) {
      getConf().set(DIRECTORY_BLACKLIST_REGEX, commandLine.getOptionValue("b"));
      LOG.info("Blacklist:" + commandLine.getOptionValue("b"));
    }

    if (commandLine.hasOption("dry-run")) {
      return runReplicationCompareJob(commandLine.getOptionValue("source"),
          commandLine.getOptionValue("destination"), commandLine.getOptionValue("output-path"),
          commandLine.getOptionValue("operation"));
    } else {
      Path outputRoot = new Path(commandLine.getOptionValue("output-path")).getParent();
      String tmpPath =
          outputRoot.toString() + "/__tmp_hive_result_." + System.currentTimeMillis();
      int retVal = 1;

      if (runReplicationCompareJob(commandLine.getOptionValue("source"),
          commandLine.getOptionValue("destination"), tmpPath,
              commandLine.getOptionValue("operation")) == 0) {
        Path tmpDirectory = new Path(tmpPath);
        FileSystem fs = tmpDirectory.getFileSystem(getConf());
        if (!fs.exists(tmpDirectory)) {
          LOG.error(tmpDirectory.toString() + " directory does not exist");
          return 1;
        }
        LOG.info("output exists: " + fs.getFileStatus(tmpDirectory).toString());
        retVal = runSyncJob(commandLine.getOptionValue("source"),
                commandLine.getOptionValue("destination"), tmpPath + "/part*",
                commandLine.getOptionValue("output-path"));
      }

      Path tmpDirectory = new Path(tmpPath);
      FileSystem fs = tmpDirectory.getFileSystem(getConf());
      if (fs.exists(tmpDirectory)) {
        fs.delete(tmpDirectory, true);
      }
      return retVal;
    }
  }

  private int runReplicationCompareJob(String source, String destination, String output,
      String compareOption) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "Replication Compare job");
    job.setJarByClass(getClass());

    job.setInputFormatClass(DirScanInputFormat.class);
    job.setMapperClass(ListFileMapper.class);

    job.setReducerClass(DirectoryCompareReducer.class);

    // last directory is destination, all other directories are source directory
    job.getConfiguration().set(SRC_PATH_CONF, source);
    job.getConfiguration().set(DST_PATH_CONF, destination);
    job.getConfiguration().set("mapred.input.dir", Joiner.on(",").join(source, destination));
    job.getConfiguration().set(COMPARE_OPTION_CONF, compareOption);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FileStatus.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private int runSyncJob(String source, String destination, String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "HDFS Sync job");
    job.setJarByClass(getClass());

    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(HdfsSyncMapper.class);
    job.setReducerClass(HdfsSyncReducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.getConfiguration().set(SRC_PATH_CONF, source);
    job.getConfiguration().set(DST_PATH_CONF, destination);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileInputFormat.setMaxInputSplitSize(job,
            this.getConf().getLong( FileInputFormat.SPLIT_MAXSIZE, 60000L));
    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ReplicationJob(), args);
    System.exit(res);
  }
}
