package com.airbnb.di.hive.batchreplication.hdfscopy;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.genValue;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import com.airbnb.di.hive.batchreplication.ExtendedFileStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Map/Reduce job that takes in gzipped json log file (like the kind that are dumped out by flog),
 * splits them based on the key data.event_name and writes out the results to gzipped files split on
 * event name.
 */
public class HdfsStatus extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(HdfsStatus.class);
  private static final String SRC_HOSTNAME_CONF = "replication.src.hostname";
  private static final String DST_HOSTNAME_CONF = "replication.dst.hostname";
  private static final String COMPARE_OPTION_CONF = "replication.compare.option";
  public static final String DIRECTORY_BLACKLIST_REGEX = "replication.folder.blacklist";

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private static String getHostName(String path) {
    String[] parts = path.split("/");
    assert parts.length > 3;

    // return host name
    return parts[2];
  }

  public static class ListFileMapper extends Mapper<Text, Boolean, Text, Text> {
    private String folderBlackList;
    private boolean nofilter;

    private static String getPathNoHostName(String path) {
      String[] parts = path.split("/");
      assert parts.length > 3;

      return "/" + Joiner.on("/").join(Arrays.copyOfRange(parts, 3, parts.length));
    }

    private long[] enumDirectories(FileSystem fs, Path root, boolean recursive, Context context)
        throws IOException {
      try {
        // keep track of file couter and total size
        long[] folderStats = new long[2];
        for (FileStatus status : nofilter ? fs.listStatus(root)
            : fs.listStatus(root, hiddenFileFilter)) {
          if (status.isDir()) {
            if (recursive) {
              if (folderBlackList == null || !status.getPath().getName().matches(folderBlackList)) {
                long[] subFolderStats = enumDirectories(fs, status.getPath(), recursive, context);
                folderStats[0] += subFolderStats[0];
                folderStats[1] += subFolderStats[1];
              }
            }
          } else {
            folderStats[0]++;
            folderStats[1] += status.getLen();
          }
        }

        try {
          String relPath = getPathNoHostName(root.toString());
          int relativeIndex = relPath.lastIndexOf('/');
          String parentPath = relativeIndex == -1 ? "/" : relPath.substring(0, relativeIndex);

          context.write(new Text(parentPath),
              new Text(genValue(relPath, String.valueOf(folderStats[0]),
                  String.valueOf(folderStats[1]), String.valueOf(recursive))));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        context.progress();

        return folderStats;
      } catch (FileNotFoundException e) {
        return null;
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.folderBlackList = context.getConfiguration().get(DIRECTORY_BLACKLIST_REGEX);
      this.nofilter =
          context.getConfiguration().getBoolean(DirScanInputFormat.NO_FILE_FILTER, false);
    }

    @Override
    protected void map(Text key, Boolean value, Context context)
        throws IOException, InterruptedException {
      Path folder = new Path(key.toString());
      FileSystem fileSystem = folder.getFileSystem(context.getConfiguration());
      enumDirectories(fileSystem, folder, value, context);
      LOG.info(key.toString() + " processed.");
    }
  }

  /**
   * Generate hdfs file statistics table for hive.
   */
  public static class FolderSizeReducer extends Reducer<Text, Text, Text, Text> {
    private static String produceHdfsStats(FileStatus fileStatus) {
      ArrayList<String> fields = new ArrayList<>();

      String[] parts = fileStatus.getPath().toString().split("/");
      assert parts.length > 3;

      // add host name
      fields.add(parts[2]);

      // add relative path
      fields.add("/" + Joiner.on("/").join(Arrays.copyOfRange(parts, 3, parts.length)));

      // add level up to 10
      fields.addAll(Arrays.asList(Arrays.copyOfRange(parts, 3, 13)));

      // add file size
      fields.add(String.valueOf(fileStatus.getLen()));

      // add block size
      fields.add(String.valueOf(fileStatus.getBlockSize()));

      // add owner
      fields.add(String.valueOf(fileStatus.getOwner()));

      // add group
      fields.add(String.valueOf(fileStatus.getGroup()));

      // add permission
      fields.add(String.valueOf(fileStatus.getModificationTime()));

      // add EveryThing
      fields.add(fileStatus.toString());

      return Joiner.on("\t").useForNull("\\N").join(fields);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        String[] fields = value.toString().split("\t");

        // context.write(new Text(fs.getPath().toString()), new Text(produceHdfsStats(fs)));
      }
    }
  }

  private static Text generateValue(String action, ExtendedFileStatus fileStatus) {
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
  public static class FolderCompareReducer extends Reducer<Text, FileStatus, Text, Text> {
    private String dstHost;
    private Predicate<ExtendedFileStatus> dstHostPred;
    private HashSet<String> compareOption;

    private ExtendedFileStatus findSrcFileStatus(List<ExtendedFileStatus> fileStatuses) {
      // pick copy source. The source is the one with largest timestamp value
      return Ordering.from(new Comparator<ExtendedFileStatus>() {
        @Override
        public int compare(ExtendedFileStatus o1, ExtendedFileStatus o2) {
          return Long.compare(o1.getModificationTime(), o2.getModificationTime());
        }
      }).max(fileStatuses);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      this.dstHost = context.getConfiguration().get(DST_HOSTNAME_CONF);
      this.dstHostPred = new Predicate<ExtendedFileStatus>() {
        @Override
        public boolean apply(@Nullable ExtendedFileStatus extendedFileStatus) {
          return extendedFileStatus.getHostName().equals(dstHost);
        }
      };
      this.compareOption = new HashSet<>();
      this.compareOption.addAll(
          Arrays.asList(context.getConfiguration().get(COMPARE_OPTION_CONF, "a,d,u").split(",")));
    }

    @Override
    protected void reduce(Text key, Iterable<FileStatus> values, Context context)
        throws IOException, InterruptedException {
      ListMultimap<String, ExtendedFileStatus> fileStatusHashMap = LinkedListMultimap.create();

      for (FileStatus fs : values) {
        ExtendedFileStatus efs =
            new ExtendedFileStatus(fs.getPath().toString(), fs.getLen(), fs.getModificationTime());
        fileStatusHashMap.put(efs.getPath(), efs);
      }

      for (String relativePath : fileStatusHashMap.keySet()) {
        List<ExtendedFileStatus> fileStatuses = fileStatusHashMap.get(relativePath);
        ArrayList<ExtendedFileStatus> srcFileStatus =
            Lists.newArrayList(Iterables.filter(fileStatuses, Predicates.not(this.dstHostPred)));
        ArrayList<ExtendedFileStatus> dstFileStatus =
            Lists.newArrayList(Iterables.filter(fileStatuses, this.dstHostPred));

        if (dstFileStatus.size() > 0) {
          // we can only have one destination
          assert dstFileStatus.size() == 1;

          // if destination has file, there are two cases:
          if (srcFileStatus.size() > 0) {
            // pick copy source first. The source is the one with largest timestamp value
            ExtendedFileStatus finalSrcFileStatus = findSrcFileStatus(srcFileStatus);

            // if file size is
            if (finalSrcFileStatus.getFileSize() != dstFileStatus.get(0).getFileSize()) {
              if (compareOption.contains("u")) {
                // if file size is different then we need update.
                context.write(new Text(relativePath), generateValue("update", finalSrcFileStatus));
              }
            }
          } else {
            if (compareOption.contains("d")) {
              // 2. source does not exist it is delete
              context.write(new Text(relativePath), generateValue("delete", dstFileStatus.get(0)));
            }
          }
        } else {
          if (compareOption.contains("a")) {
            // if no destination, then this is a new file.
            ExtendedFileStatus src = findSrcFileStatus(srcFileStatus);
            context.write(new Text(relativePath), generateValue("add", src));
          }
        }
      }
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
   * Construct and provide GNU-compatible Options.
   *
   * @return Options expected from command-line of GNU form.
   */
  public static Options constructGnuOptions() {
    final Options gnuOptions = new Options();
    gnuOptions.addOption("s", "source", true, "source folders").addOption("o", "output", true,
        "output folder");
    return gnuOptions;
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
    final CommandLineParser cmdLineGnuParser = new GnuParser();

    final Options gnuOptions = constructGnuOptions();
    CommandLine commandLine;
    try {
      commandLine = cmdLineGnuParser.parse(gnuOptions, args);
    } catch (ParseException parseException) { // checked exception
      System.err.println(
          "Encountered exception while parsing using GnuParser:\n" + parseException.getMessage());
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

    return runHdfsStatsJob(commandLine.getOptionValue("s"), commandLine.getOptionValue("o"));
  }

  private int runHdfsStatsJob(String source, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "HDFS stats job");
    job.setJarByClass(getClass());

    job.setInputFormatClass(DirScanInputFormat.class);
    job.setMapperClass(ListFileMapper.class);

    job.setReducerClass(FolderSizeReducer.class);
    job.getConfiguration().set("mapred.input.dir", source);
    job.getConfiguration().setBoolean(DirScanInputFormat.NO_FILE_FILTER, true);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FileStatus.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private static String getSourceHosts(String source) {
    String[] srcfolders = source.split(",");

    return Joiner.on(",")
        .join(Lists.transform(Arrays.asList(srcfolders), new Function<String, String>() {
          @Override
          public String apply(String str) {
            return getHostName(str);
          }
        }));
  }

  private int runReplicationCompareJob(String source, String destination, String output,
      String compareOption) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "Replication Compare job");
    job.setJarByClass(getClass());

    job.setInputFormatClass(DirScanInputFormat.class);
    job.setMapperClass(ListFileMapper.class);

    job.setReducerClass(FolderCompareReducer.class);

    // last folder is destination, all other folders are source folder
    job.getConfiguration().set(SRC_HOSTNAME_CONF, getSourceHosts(source));
    job.getConfiguration().set(DST_HOSTNAME_CONF, getHostName(destination));
    job.getConfiguration().set("mapred.input.dir", Joiner.on(",").join(source, destination));
    job.getConfiguration().set(COMPARE_OPTION_CONF, compareOption);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FileStatus.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HdfsStatus(), args);
    System.exit(res);
  }
}
