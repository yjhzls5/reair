package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.batchreplication.ExtendedFileStatus;
import com.airbnb.di.hive.batchreplication.ReplicationUtils;
import com.airbnb.di.hive.batchreplication.hivecopy.MetastoreCompareUtils.UpdateAction;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.ThriftHiveMetastoreClient;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
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

import javax.annotation.Nullable;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.genValue;
import static com.airbnb.di.hive.batchreplication.ReplicationUtils.getClusterName;
import static com.airbnb.di.hive.batchreplication.ReplicationUtils.removeOutputDirectory;
import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreCompareUtils.UpdateAction.RECREATE;
import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreCompareUtils.UpdateAction.UPDATE;

public class MetastoreReplicationJobV2 extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MetastoreReplicationJobV2.class);

    public static final String REPLICATION_METASTORE_BLACKLIST = "replication.metastore.blacklist";
    public static final String REPLICATION_METASTORE_COPYFROM = "replication.metastore.copyfrom";
    public static final String REPLICATION_METASTORE_COPYTO = "replication.metastore.copyto";
    private static final String REPLICATION_METASTORE_SRC_HOST = "replication.metastore.src.host";
    private static final String REPLICATION_METASTORE_DST_HOST = "replication.metastore.dst.host";
    public static final String REPLICATION_ALLOW_S3 = "replication.metastore.s3tablesync";

    private static final ImmutableMap<String, String> HDFSPATH_CHECK_MAP =
            ImmutableMap.of("brain", "airfs-brain", "gold", "airfs-gold");
    private static final ImmutableMap<String, String> NAMENODE_MAP =
            ImmutableMap.of("brain", "airfs-brain", "silver", "airfs-silver", "gold", "airfs-gold");
    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };
    public static final String USAGE_COMMAND_STR = "Usage: hadoop jar ReplicationJob-0.1-job.jar " +
            "com.airbnb.replication.hivecopy.MetastoreReplicationJobV2 -libjar <path hive-metastore.jar> options";
    public static final String REPLICATED_FROM_PINKY_BRAIN = "ReplicatedFromPinkyBrain";

    /**
     * Merge destination table parameters with source table parameters and stored it in source Table object.
     * Basically we copy parameter does not exist in source from destination into source table object.
     * Overlapped parameters will use from source table.
     */
    private static void mergeParameters(Table src, Table dst) {
        MapDifference<String, String> paraDiff = Maps.difference(src.getParameters(), dst.getParameters());
        for(Map.Entry<String, String> value : paraDiff.entriesOnlyOnRight().entrySet()) {
            src.putToParameters(value.getKey(), value.getValue());
        }
    }

    private static void mergeParameters(Partition src, Partition dst) {
        MapDifference<String, String> paraDiff = Maps.difference(src.getParameters(), dst.getParameters());
        for(Map.Entry<String, String> value : paraDiff.entriesOnlyOnRight().entrySet()) {
            src.putToParameters(value.getKey(), value.getValue());
        }
    }

    private static boolean hdfsCleanFolder(String db, String table, String part, String dst, Configuration conf, boolean recreate) {
        Path dstPath = new Path(dst);

        try {
            FileSystem fs = dstPath.getFileSystem(conf);
            if(fs.exists(dstPath) && !fs.delete(dstPath, true)) {
                throw new IOException("Failed to delete dstFolder:" + dstPath.toString());
            }

            if(fs.exists(dstPath)) {
                throw new IOException("Validate delete dstFolder failed:" + dstPath.toString());
            }

            if (!recreate) {
                return true;
            }

            fs.mkdirs(dstPath);

            if(!fs.exists(dstPath)) {
                throw new IOException("Validate recreate dstFolder failed:" + dstPath.toString());
            }

        } catch (IOException e) {
            LOG.info(String.format("%s:%s:%s error=%s", db, table, part, e.getMessage()));
            LOG.info("path:" + dst);
            return false;
        }

        return true;
    }

    public enum MetastoreAction {
        CREATE_TABLE,  // create partition table and view. This will be done before hdfs copy.
        CREATE_TABLE_NONPART, // create non partition table. This needs to be done after hdfs copy is done.
        SAME_META_TABLE,
        CHECK_DIRECTORY_TABLE,
        SKIP_TABLE,
        UPDATE_TABLE,
        RECREATE_TABLE,
        CREATE_PARTITION,
        UPDATE_PARTITION,
        DROP_PARTITION
    }

    public MetastoreReplicationJobV2() {
    }

    private static String hdfsFileChanged(String db, String table, String part, HdfsPath src, HdfsPath dst, Configuration conf)
            throws IOException {
        String srcClusterName = getClusterName(src);
        String dstClusterName = getClusterName(dst);

        if(srcClusterName.equals("s3")) {
            if (dstClusterName.equals("s3")) {
                return null;
            } else {
                return "archived";
            }
        } else if(srcClusterName.equals("unknown")) {
            return dstClusterName.equals(srcClusterName)?null:Joiner.on(",").join(src.getFullPath(), dst.getFullPath());
        } else {
            Path srcPath = new Path(src.replaceHost(NAMENODE_MAP.get(srcClusterName)));
            Path dstPath = new Path(dst.replaceHost(NAMENODE_MAP.get(dstClusterName)));
            FileSystem srcFs = null;
            FileSystem dstFs = null;
            FileStatus[] srcList = null;
            FileStatus[] dstList = null;
            try {
                srcFs = srcPath.getFileSystem(conf);
                dstFs = dstPath.getFileSystem(conf);
                srcList = srcFs.listStatus(srcPath, hiddenFileFilter);
                dstList = dstFs.listStatus(dstPath, hiddenFileFilter);
            } catch (Exception e) {
                LOG.info("dstHdfsPath:" + dst.getFullPath());
                LOG.info("srcPath:" + srcPath.toString());
                LOG.info("destPath:" + dstPath.toString());
            }

            if (srcFs == null || dstFs == null) {
                LOG.info("source or dest FileSystem is null");
                return null;
            }

            if (srcList == null) {
                LOG.info("source path does not exist skip.");
                return null;
            }

            if (dstList == null) {
                LOG.info("dest path does not exist copy.");
                return "dest path does not exist.";
            }

            try {
                HashSet<String> e = Sets.newHashSet(
                        Lists.transform(Arrays.asList(srcList),
                                new Function<FileStatus, String>() {
                                    @Nullable
                                    public String apply(FileStatus fileStatus) {
                                        return fileStatus.getPath().getName() + ":" + fileStatus.getLen() + ":" + fileStatus.getModificationTime();
                                    }
                                }));
                HashSet<String> dstFileInfo = Sets.newHashSet(
                        Lists.transform(Arrays.asList(dstList),
                                new Function<FileStatus, String>() {
                                    @Nullable
                                    public String apply(FileStatus fileStatus) {
                                        return fileStatus.getPath().getName() + ":" + fileStatus.getLen() + ":" + fileStatus.getModificationTime();
                                    }
                                }));
                SetView<String> diff = Sets.symmetricDifference(e, dstFileInfo);
                return diff.isEmpty() ? null : diff.toString();
            } catch (Exception e) {
                LOG.info(String.format("%s:%s:%s error=%s", db, table, part, e.getMessage()));
                LOG.info("source path:" + src.getFullPath() + "," + srcPath.toString());
                LOG.info("dest path:" + dst.getFullPath() + "," + dstPath.toString());
                return null;
            }
        }
    }

    public static void printUsage(String applicationName, Options options, OutputStream out) {
        PrintWriter writer = new PrintWriter(out);
        HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printUsage(writer, 80, applicationName, options);
        writer.flush();
    }

    public static Options constructGnuOptions() {
        Options gnuOptions = new Options();
        gnuOptions.addOption("s", "source", true, "source metastore url").
                addOption("d", "destination", true, "destination metastore url").
                addOption("o", "output", true, "logging output folder").
                addOption("ms", "metastore", true, "source metastore name").
                addOption("dms", "dest-metastore", true, "destination metastore name").
                addOption("b", "blacklist", true, "folder blacklist regex").
                addOption("st", "step", true, "run individual step").
                addOption("s3", "s3", false, "allow s3 backed table sync").
                addOption("oi", "input", true, "override input for a step").
                addOption("tl", "inputtablelist", true, "take pre-generated list of table instead of scan metastore");
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

        if (!commandLine.hasOption("s") || !commandLine.hasOption("d") ||
                !commandLine.hasOption("o") || !commandLine.hasOption("ms")) {
            System.err.println("-s, -d, -o, -ms options are required.");
            printUsage(USAGE_COMMAND_STR, constructGnuOptions(), System.out);
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }

        if (commandLine.hasOption("b")) {
            this.getConf().set(REPLICATION_METASTORE_BLACKLIST, commandLine.getOptionValue("b"));
            LOG.info("Blacklist:" + commandLine.getOptionValue("b"));
        }

        String dstMetastore = "silver";
        if (commandLine.hasOption("dms")) {
            dstMetastore = commandLine.getOptionValue("dms");
        }

        if (commandLine.hasOption("s3")) {
            this.getConf().setBoolean(REPLICATION_ALLOW_S3, true);
            LOG.info("allow s3 backed table sync");
        }

        int step = -1;
        if (commandLine.hasOption("st")) {
            step = Integer.valueOf(commandLine.getOptionValue("st"));
        }

        String finalOutput = commandLine.getOptionValue("o");
        Path outputParent = new Path(finalOutput);
        String step1Out = outputParent + "/step1output";
        String step2Out = outputParent + "/step2output";
        String step3Out = outputParent + "/step3output";
        if (step == -1) {
            removeOutputDirectory(step1Out, this.getConf());
            removeOutputDirectory(step2Out, this.getConf());
            removeOutputDirectory(step3Out, this.getConf());
            int result = 0;
            if (commandLine.hasOption("tl")) {
                result = this.runMetastoreCompareJobWithTextInput(commandLine.getOptionValue("s"),
                        commandLine.getOptionValue("d"), commandLine.getOptionValue("tl"),
                        step1Out, commandLine.getOptionValue("ms"), dstMetastore);
            } else {
                result = this.runMetastoreCompareJob(commandLine.getOptionValue("s"),
                        commandLine.getOptionValue("d"),
                        step1Out, commandLine.getOptionValue("ms"), dstMetastore);
            }
            return result == 0 &&
                    this.runHdfsCopyJob(commandLine.getOptionValue("s"),
                            commandLine.getOptionValue("d"), step1Out + "/part*",
                            step2Out,
                            commandLine.getOptionValue("ms"), dstMetastore) == 0 ? this
                    .runCommitChangeJob(commandLine.getOptionValue("s"),
                            commandLine.getOptionValue("d"), step1Out + "/part*", step3Out,
                            commandLine.getOptionValue("ms"), dstMetastore) : 1;
        } else {
            switch (step) {
            case 1:
                removeOutputDirectory(step1Out, this.getConf());
                if (commandLine.hasOption("tl")) {
                    return this.runMetastoreCompareJobWithTextInput(commandLine.getOptionValue("s"),
                            commandLine.getOptionValue("d"), commandLine.getOptionValue("tl"), step1Out,
                            commandLine.getOptionValue("ms"), dstMetastore);
                } else {
                    return this.runMetastoreCompareJob(commandLine.getOptionValue("s"),
                            commandLine.getOptionValue("d"), step1Out, commandLine.getOptionValue("ms"), dstMetastore);
                }
            case 2:
                removeOutputDirectory(step2Out, this.getConf());
                if (commandLine.hasOption("oi")) {
                    step1Out = commandLine.getOptionValue("oi");
                }

                return this.runHdfsCopyJob(commandLine.getOptionValue("s"), commandLine.getOptionValue("d"),
                        step1Out + "/part*", step2Out, commandLine.getOptionValue("ms"), dstMetastore);
            case 3:
                removeOutputDirectory(step3Out, this.getConf());
                if (commandLine.hasOption("oi")) {
                    step1Out = commandLine.getOptionValue("oi");
                }

                return this.runCommitChangeJob(commandLine.getOptionValue("s"),
                        commandLine.getOptionValue("d"),
                        step1Out + "/part*", step3Out, commandLine.getOptionValue("ms"), dstMetastore);
            default:
                LOG.info("Invalid steps specified:" + step);
                return 1;
            }
        }
    }

    private int runMetastoreCompareJob(String source, String destination, String output, String metastore, String destMetastore)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "metastore Compare job");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(MetastoreScanInputFormat.class);
        job.setMapperClass(ProcessTableMapper.class);
        job.setReducerClass(CheckPartitionReducer.class);
        job.getConfiguration().set(REPLICATION_METASTORE_SRC_HOST, source);
        job.getConfiguration().set(REPLICATION_METASTORE_DST_HOST, destination);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYFROM, metastore);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYTO, destMetastore);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        boolean success = job.waitForCompletion(true);
        return success?0:1;
    }

    private int runMetastoreCompareJobWithTextInput(String source, String destination, String input, String output, String metastore, String destMetastore)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "metastore Compare job2");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ProcessTableMapperWithTextInput.class);
        job.setReducerClass(CheckPartitionReducer.class);
        job.getConfiguration().set(REPLICATION_METASTORE_SRC_HOST, source);
        job.getConfiguration().set(REPLICATION_METASTORE_DST_HOST, destination);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYFROM, metastore);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYTO, destMetastore);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileInputFormat.setMaxInputSplitSize(job, 6000L);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        boolean success = job.waitForCompletion(true);
        return success?0:1;
    }

    private int runHdfsCopyJob(String source, String destination, String input, String output, String metastore, String destMetastore)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "hdfs copy job");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HdfsCopyMapper.class);
        job.setReducerClass(HdfsCopyReducer.class);
        job.getConfiguration().set(REPLICATION_METASTORE_SRC_HOST, source);
        job.getConfiguration().set(REPLICATION_METASTORE_DST_HOST, destination);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYFROM, metastore);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYTO, destMetastore);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileInputFormat.setMaxInputSplitSize(job, 60000L);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        boolean success = job.waitForCompletion(true);
        return success?0:1;
    }

    private int runCommitChangeJob(String source, String destination, String input, String output, String metastore, String destMetastore)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(this.getConf(), "Commit Change Job");
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CommitChangeMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set(REPLICATION_METASTORE_SRC_HOST, source);
        job.getConfiguration().set(REPLICATION_METASTORE_DST_HOST, destination);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYFROM, metastore);
        job.getConfiguration().set(REPLICATION_METASTORE_COPYTO, destMetastore);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileInputFormat.setMaxInputSplitSize(job, 60000L);
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        boolean success = job.waitForCompletion(true);
        return success?0:1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MetastoreReplicationJobV2(), args);
        System.exit(res);
    }

    public static class CommitChangeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private HiveMetastoreClient srcClient;
        private HiveMetastoreClient dstClient;
        private String dstMetasotre;
        private Configuration conf;
        private boolean allowS3TableSync;

        public CommitChangeMapper() {
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String[] srcHostParts = context.getConfiguration().get(REPLICATION_METASTORE_SRC_HOST).split(":");
                this.srcClient = new ThriftHiveMetastoreClient(srcHostParts[0], Integer.valueOf(srcHostParts[1]));
                String[] dstHostParts = context.getConfiguration().get(REPLICATION_METASTORE_DST_HOST).split(":");
                this.dstClient = new ThriftHiveMetastoreClient(dstHostParts[0], Integer.valueOf(dstHostParts[1]));
                this.allowS3TableSync = context.getConfiguration().getBoolean(REPLICATION_ALLOW_S3, false);
                this.dstMetasotre = context.getConfiguration().get(REPLICATION_METASTORE_COPYTO, "silver");
                this.conf = context.getConfiguration();
            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] columns = value.toString().split("\t");
                MetastoreAction action = Enum.valueOf(MetastoreAction.class, columns[0]);
                String db = columns[1];
                String table = columns[2];
                String opt = columns.length > 3?columns[3]:"NULL";
                String result = "";
                if(columns.length > 4) {
                    result = columns[4];
                }

                String re;
                Table tab = srcClient.getTable(db, table);
                HdfsPath tablePath = null;
                if (tab != null) {
                    if (!tab.getTableType().contains("VIEW")) {
                        tablePath = new HdfsPath(tab.getSd().getLocation());
                        if (!getClusterName(tablePath).equals("s3")) {
                            tab.getSd().setLocation(tablePath.replaceHost(NAMENODE_MAP.get(dstMetasotre)));
                        }
                    }
                }
                switch(action) {
                case CREATE_TABLE:
                case SAME_META_TABLE:
                case SKIP_TABLE:
                case RECREATE_TABLE:
                default:
                    break;

                case CREATE_TABLE_NONPART:
                    if (tab != null) {
                        if (dstClient.getDatabase(db) == null) {
                            Database srcDb = srcClient.getDatabase(db);

                            HdfsPath dbPath = new HdfsPath(srcDb.getLocationUri());
                            srcDb.setLocationUri(dbPath.replaceHost(NAMENODE_MAP.get(dstMetasotre)));
                            dstClient.createDatabase(srcDb);
                        }

                        Table dstTable = dstClient.getTable(db, table);
                        if (dstTable != null) {
                            context.write(value, new Text("nonpart table create skipped due to table already exists."));
                            break;
                        }

                        if (tablePath!=null && tablePath.getProto().contains("s3")) {
                            if (!allowS3TableSync) {
                                context.write(value, new Text("s3 backed nonpart table create skipped"));
                                LOG.info("Skip create table due to s3 table " + db + ":" + table);
                            } else {
                                tab.getSd().setLocation(null);
                                dstClient.createTable(tab);
                                tab.getSd().setLocation(tablePath.getFullPath());
                                dstClient.alterTable(db, table, tab);
                                context.write(value, new Text(
                                        "s3 backed nonpart table created"));
                                LOG.info("s3 backed nonpart table created " + db + ":" + table);
                            }
                        } else {
                            re = hdfsFileChanged(db, table, "", tablePath,
                                    new HdfsPath(tab.getSd().getLocation()), conf);
                            if (re != null) {
                                hdfsCleanFolder(db, table, opt, tab.getSd().getLocation(), conf, false);
                                context.write(value, new Text(
                                        "create nonpartition table aborted because it is changed since it is copied " +
                                                re));
                            } else {
                                dstClient.createTable(tab);
                                context.write(value, new Text("nonpartition table created"));
                                LOG.info("Create table on destination" + db + ":" + table);
                            }
                        }
                    } else {
                        context.write(value, new Text("nonpart table create skipped"));
                        LOG.info("Skip create table due to source deleted " + db + ":" + table);
                    }
                    break;
                case UPDATE_TABLE:
                    if (tab != null) {
                        if (!allowS3TableSync && tablePath!=null && tablePath.getProto().contains("s3")) {
                            context.write(value, new Text("s3 backed table update skipped"));
                            LOG.info("Skip update table due to s3 table " + db + ":" + table);
                        } else {
                            tab.putToParameters(REPLICATED_FROM_PINKY_BRAIN, "true");

                            try {
                                Table dstTable = dstClient.getTable(db, table);
                                if (dstTable == null) {
                                    context.write(value, new Text("update table skipped due to source deleted"));
                                    break;
                                }

                                mergeParameters(tab, dstTable);

                                dstClient.alterTable(db, table, tab);
                                LOG.info("Update table on destination" + db + ":" + table);
                                context.write(value, new Text("update table metadata"));
                                break;
                            } catch (HiveMetastoreException e) {
                                LOG.info(e.getMessage());
                                LOG.info("retry with receate.");
                            }

                            // retry with recreate table
                            dstClient.dropTable(db, table, false);
                            dstClient.createTable(tab);
                            LOG.info("recreate table on destination" + db + ":" + table);
                            context.write(value, new Text("recreate table metadata"));
                        }
                    } else {
                        context.write(value, new Text("update table skipped due to source deleted"));
                    }
                    break;

                case CREATE_PARTITION:
                    Partition p1 = srcClient.getPartition(db, table, opt);
                    if (p1 != null) {
                        Partition p2 = dstClient.getPartition(db, table, opt);
                        if (p2 != null) {
                            context.write(value, new Text("partition already created: skipped"));
                            break;
                        }

                        HdfsPath partitionPath = new HdfsPath(p1.getSd().getLocation());
                        if (!getClusterName(partitionPath).equals("s3")) {
                            p1.getSd().setLocation(partitionPath.replaceHost(NAMENODE_MAP.get(dstMetasotre)));
                        } else if (allowS3TableSync) {
                            // s3 need to create partition first then set path otherwise aws will be overwhelm
                            p1.getSd().setLocation(null);
                            try {
                                dstClient.addPartition(p1);
                                p1.getSd().setLocation(partitionPath.getFullPath());
                                dstClient.alterPartition(db, table, p1);
                            } catch (HiveMetastoreException e) {
                                LOG.info("Race condition. Partition already created." + e.getMessage());
                            }
                            context.write(value, new Text("s3 partition created"));
                            break;
                        }

                        if (!allowS3TableSync && partitionPath.getProto().contains("s3")) {
                            context.write(value, new Text("s3 backed partition create skipped"));
                        } else {
                            re = hdfsFileChanged(db, table, "", partitionPath,
                                    new HdfsPath(p1.getSd().getLocation()), conf);
                            if (re != null) {
                                hdfsCleanFolder(db, table, opt, p1.getSd().getLocation(), conf, false);
                                context.write(value, new Text(
                                        "create partition aborted because it is changed since it is copied " + re));
                            } else {
                                try {
                                    dstClient.addPartition(p1);
                                } catch (HiveMetastoreException e) {
                                    LOG.info("Race condition. Partition already created." + e.getMessage());
                                }
                                context.write(value, new Text("partition created"));
                            }
                        }
                    } else {
                        context.write(value, new Text("create partition skipped due to source deleted"));
                    }
                    break;
                case DROP_PARTITION:
                    Partition dstPart = dstClient.getPartition(db, table, opt);
                    if (dstPart == null) {
                        context.write(value, new Text("partition already dropped"));
                        break;
                    }
                    HdfsPath partHdfsPath = new HdfsPath(dstPart.getSd().getLocation());
                    if (!allowS3TableSync && partHdfsPath.getProto().contains("s3")) {
                        context.write(value, new Text("s3 backed partition create skipped"));
                    } else {
                        try {
                            dstClient.dropPartition(db, table, opt, true);
                            context.write(value, new Text("partition dropped"));

                        } catch (HiveMetastoreException e) {
                            LOG.info("failed to drop partition." + e.getMessage());
                            context.write(value, new Text("drop partition failed"));
                        }
                    }
                    break;
                case UPDATE_PARTITION:
                    Partition p11 = this.srcClient.getPartition(db, table, opt);
                    Partition p21 = this.dstClient.getPartition(db, table, opt);
                    if(p11 != null && p21 != null) {
                        HdfsPath p11HdfsPath = new HdfsPath(p11.getSd().getLocation());

                        if (!allowS3TableSync && !result.contains("archived") && p11HdfsPath.getProto().contains("s3")) {
                            context.write(value, new Text("update partition skipped due to s3 skipping."));
                        } else {
                            // if folder is copied, validate if it is correct.
                            if(!result.contains("archived") && result.contains("different")) {
                                re = hdfsFileChanged(db, table, opt, new HdfsPath(p11.getSd().getLocation()),
                                        new HdfsPath(p21.getSd().getLocation()), this.conf);
                                if (re != null) {
                                    dstClient.dropPartition(db, table, opt, true);
                                    context.write(value, new Text(
                                            "partition deleted because it is changed since it is copied " + re));
                                    break;
                                }
                            }

                            HdfsPath hdfsPath = new HdfsPath(p11.getSd().getLocation());
                            if (!getClusterName(hdfsPath).equals("s3")) {
                                p11.getSd().setLocation(hdfsPath.replaceHost(NAMENODE_MAP.get(dstMetasotre)));
                            }

                            MetastoreCompareUtils.UpdateAction compareResult =
                                    MetastoreCompareUtils.ComparePartitionForReplication(p11, p21);
                            mergeParameters(p11, p21);
                            if (compareResult == UPDATE) {
                                dstClient.alterPartition(db, table, p11);
                                context.write(value, new Text(
                                        "partition update succeeded"));
                            } else if (compareResult == RECREATE) {
                                dstClient.dropPartition(db, table, opt, false);
                                dstClient.addPartition(p11);
                                context.write(value, new Text("partition recreated succeeded"));

                            } else {
                                context.write(value, new Text("file only update succeeded"));
                            }
                        }
                    }
                    break;
                case CHECK_DIRECTORY_TABLE:
                    if(!result.contains("archived") && result.contains("different")) {
                        Table table1 = this.srcClient.getTable(db, table);
                        Table table2 = this.dstClient.getTable(db, table);
                        if(table1 != null && table2 != null) {
                            re = hdfsFileChanged(db, table, "", new HdfsPath(table1.getSd().getLocation()),
                                    new HdfsPath(table2.getSd().getLocation()), this.conf);
                            if(re != null) {
                                dstClient.dropTable(db, table, true);
                                context.write(value, new Text("non-partition table deleted because it is changed since it is copied " + re));
                            } else {
                                context.write(value, new Text("copy succeeded"));
                            }
                        }
                    }
                    break;
                }
            } catch (HiveMetastoreException e) {
                LOG.info(String.format("%s got exception", value.toString()));
                LOG.info(e.getMessage());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.srcClient.close();
            this.dstClient.close();
        }
    }

    public static class HdfsCopyReducer extends Reducer<LongWritable, Text, Text, Text> {
        private Configuration conf;

        public HdfsCopyReducer() {
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
        }

        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value: values) {
                String[] fields = value.toString().split("\t");
                String srcFileName = fields[0];
                String dstFolder = fields[1];
                long size = Long.valueOf(fields[2]);
                ExtendedFileStatus fileStatus = new ExtendedFileStatus(srcFileName, size, 0L);
                FileSystem srcFs = (new Path(srcFileName)).getFileSystem(this.conf);
                FileSystem dstFs = (new Path(dstFolder)).getFileSystem(this.conf);
                String result = ReplicationUtils.doCopyFileAction(fileStatus, srcFs, dstFolder, dstFs, context, false, context.getTaskAttemptID().toString());
                if(result == null) {
                    context.write(new Text("copied"), new Text(genValue(value.toString(), " ", String.valueOf(System.currentTimeMillis()))));
                } else {
                    context.write(new Text("skip copy"), new Text(genValue(value.toString(), result, String.valueOf(System.currentTimeMillis()))));
                }
            }
        }
    }

    public static class HdfsCopyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private HiveMetastoreClient srcClient;
        private HiveMetastoreClient dstClient;
        private String metastore;
        private String dstMetastore;
        private Configuration conf;

        public HdfsCopyMapper() {
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String[] srcHostParts = context.getConfiguration().get(REPLICATION_METASTORE_SRC_HOST).split(":");
                this.srcClient = new ThriftHiveMetastoreClient(srcHostParts[0], Integer.valueOf(srcHostParts[1]).intValue());
                String[] dstHostParts = context.getConfiguration().get(REPLICATION_METASTORE_DST_HOST).split(":");
                this.dstClient = new ThriftHiveMetastoreClient(dstHostParts[0], Integer.valueOf(dstHostParts[1]).intValue());
                this.metastore = context.getConfiguration().get(REPLICATION_METASTORE_COPYFROM, "brain");
                this.dstMetastore = context.getConfiguration().get(REPLICATION_METASTORE_COPYTO, "silver");
                this.conf = context.getConfiguration();
            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split("\t");
                MetastoreAction action = Enum.valueOf(MetastoreAction.class, fields[0]);
                String db = fields[1];
                String table = fields[2];
                String opt = fields.length > 3?fields[3]:"NULL";
                String result = "";
                if(fields.length > 4) {
                    result = fields[4];
                }

                Partition p1;
                Partition p2;
                Table t1;
                Table t2;
                switch(action) {
                case CREATE_TABLE:
                case SAME_META_TABLE:
                case SKIP_TABLE:
                case UPDATE_TABLE:
                case RECREATE_TABLE:
                case DROP_PARTITION:
                default:
                    break;

                case CREATE_TABLE_NONPART:
                    t1 = this.srcClient.getTable(db, table);
                    t2 = this.dstClient.getTable(db, table);
                    if (t1 != null && t2 == null) {
                        HdfsPath dstHdfsPath = new HdfsPath(t1.getSd().getLocation());
                        if (!getClusterName(dstHdfsPath).equals("s3")) {
                            // clean targe folder and generate copy file list.
                            UpdateDirectory(context, db, table, opt, t1.getSd().getLocation(),
                                    dstHdfsPath.replaceHost(NAMENODE_MAP.get(dstMetastore)));
                        }
                    }
                    break;
                case CREATE_PARTITION:
                    p1 = this.srcClient.getPartition(db, table, opt);
                    p2 = this.dstClient.getPartition(db, table, opt);
                    if (p1 != null && p2 == null) {
                        HdfsPath dstHdfsPath = new HdfsPath(p1.getSd().getLocation());
                        if (!getClusterName(dstHdfsPath).equals("s3")) {
                            // clean target folder and generate copy file list.
                            UpdateDirectory(context, db, table, opt, p1.getSd().getLocation(),
                                    dstHdfsPath.replaceHost(NAMENODE_MAP.get(dstMetastore)));
                        }
                    }
                    break;
                case UPDATE_PARTITION:
                    if(result.contains("different")) {
                        p1 = this.srcClient.getPartition(db, table, opt);
                        p2 = this.dstClient.getPartition(db, table, opt);
                        if(p1 != null && p2 != null) {
                            if (result.contains("archived")) {
                                HdfsPath dstHdfsPath = new HdfsPath(p2.getSd().getLocation());

                                hdfsCleanFolder(db, table, opt, dstHdfsPath.replaceHost(NAMENODE_MAP.get(dstMetastore)),
                                        this.conf, false);
                            } else {
                                UpdateDirectory(context, db, table, opt, p1.getSd().getLocation(),
                                        p2.getSd().getLocation());
                            }
                        }
                    }
                    break;
                case CHECK_DIRECTORY_TABLE:
                    if(result.contains("different")) {
                        t1 = this.srcClient.getTable(db, table);
                        t2 = this.dstClient.getTable(db, table);
                        if(t1 != null && t2 != null) {
                            if (result.contains("archived")) {
                                HdfsPath dstHdfsPath = new HdfsPath(t2.getSd().getLocation());

                                hdfsCleanFolder(db, table, opt, dstHdfsPath.replaceHost(NAMENODE_MAP.get(dstMetastore)),
                                        this.conf, false);
                            } else {
                                UpdateDirectory(context, db, table, "", t1.getSd().getLocation(),
                                        t2.getSd().getLocation());
                            }
                        }
                    }
                    break;
                }
            } catch (HiveMetastoreException e) {
                LOG.info(String.format("%s got exception", value.toString()));
                LOG.info(e.getMessage());
                throw new InterruptedException(e.getMessage());
            }
        }

        private void UpdateDirectory(Context context, String db, String table, String partition, String src, String dst)
                throws IOException, InterruptedException {
            HdfsPath dstHdfsPath = new HdfsPath(dst);
            if(!dstHdfsPath.getProto().contains("s3")) {
                if(!hdfsCleanFolder(db, table, partition, dstHdfsPath.replaceHost(NAMENODE_MAP.get(dstMetastore)), this.conf, true)) {
                    throw new InterruptedException("Failed to update directory:" + dst);
                } else {
                    HdfsPath srcHdfsPath = new HdfsPath(src);
                    String srcClusterName = getClusterName(srcHdfsPath);
                    if(!this.metastore.equals(srcClusterName) && (!this.metastore.equals("pinky") || !srcClusterName.equals("brain"))) {
                        throw new InterruptedException("Invalid source directory:" + src);
                    } else {
                        try {
                            Path srcPath = new Path(srcHdfsPath.replaceHost(NAMENODE_MAP.get(srcClusterName)));
                            FileSystem srcFs = srcPath.getFileSystem(this.conf);
                            for (FileStatus status : srcFs.listStatus(srcPath, hiddenFileFilter)) {
                                long hashValue = Hashing.murmur3_128().hashLong(
                                        (long) (Long.valueOf(status.getLen()).hashCode() *
                                                Long.valueOf(status.getModificationTime()).hashCode())).asLong();
                                context.write(new LongWritable(hashValue), new Text(
                                        genValue(status.getPath().toString(), dst, String.valueOf(status.getLen()))));
                            }
                        } catch (IOException e) {
                            // Ignore File list generate error because source directory could be removed while we
                            // enumerate it.
                            LOG.info("Src dir is removed:" + srcHdfsPath.getFullPath());
                        }

                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.srcClient.close();
            this.dstClient.close();
        }
    }

    public static class CheckPartitionReducer extends Reducer<LongWritable, Text, Text, Text> {
        private HiveMetastoreClient srcClient;
        private HiveMetastoreClient dstClient;
        private int count = 0;
        private String metastore;
        private String dstMetastore;
        private Configuration conf;
        private boolean allowS3TableSync;

        public CheckPartitionReducer() {
        }

        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
            try {
                String[] srcHostParts = context.getConfiguration().get(REPLICATION_METASTORE_SRC_HOST).split(":");
                this.srcClient = new ThriftHiveMetastoreClient(srcHostParts[0], Integer.valueOf(srcHostParts[1]).intValue());
                String[] dstHostParts = context.getConfiguration().get(REPLICATION_METASTORE_DST_HOST).split(":");
                this.dstClient = new ThriftHiveMetastoreClient(dstHostParts[0], Integer.valueOf(dstHostParts[1]).intValue());
                this.metastore = context.getConfiguration().get(REPLICATION_METASTORE_COPYFROM, "brain");
                this.dstMetastore = context.getConfiguration().get(REPLICATION_METASTORE_COPYTO, "silver");
                this.conf = context.getConfiguration();
                this.allowS3TableSync = context.getConfiguration().getBoolean(REPLICATION_ALLOW_S3, false);
            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                String[] fields = value.toString().split("\t");
                MetastoreAction action = Enum.valueOf(MetastoreAction.class, fields[0]);
                String db = fields[1];
                String table = fields[2];
                String opt = fields.length > 3?fields[3]:"NULL";
                String result = "NULL";

                try {
                    Partition pSrc;
                    Partition pDst;
                    String diff;
                    switch(action) {
                    case CREATE_PARTITION:
                        pSrc = this.srcClient.getPartition(db, table, opt);
                        if(pSrc != null) {
                            HdfsPath srcHdfsPath = new HdfsPath(pSrc.getSd().getLocation());
                            if (!allowS3TableSync && srcHdfsPath.getProto().contains("s3")) {
                                result = "partition is s3 backed skip";
                            } else {
                                if(!srcHdfsPath.getHost().matches(HDFSPATH_CHECK_MAP.get(this.metastore)) &&
                                    !srcHdfsPath.getProto().contains("s3")) {
                                    result = "partition is from different cluster";
                                } else {
                                    result = "copy new directory:" + srcHdfsPath.getFullPath();
                                }
                            }
                        } else {
                            result = "skip add partition";
                        }
                        break;
                    case UPDATE_PARTITION:
                        pSrc = this.srcClient.getPartition(db, table, opt);
                        pDst = this.dstClient.getPartition(db, table, opt);
                        if(pSrc != null && pDst != null) {
                            String srcLocation = pSrc.getSd().getLocation();
                            HdfsPath srcHdfsPath = new HdfsPath(srcLocation);
                            if(!getClusterName(srcHdfsPath).equals("s3")) {
                                pSrc.getSd().setLocation(srcHdfsPath.replaceHost(NAMENODE_MAP.get(dstMetastore)));
                            }

                            switch(MetastoreCompareUtils.ComparePartitionForReplication(pSrc, pDst)) {
                                case UPDATE:
                                    result = "update partition";
                                    break;
                                case RECREATE:
                                    result = "recreate partition";
                                    break;
                                case NO_CHANGE:
                                    result = "skip update partition";
                            }

                            try {
                                diff = hdfsFileChanged(db, table, opt, srcHdfsPath, new HdfsPath(pDst.getSd().getLocation()), this.conf);
                                if(diff != null) {
                                    result = result + " partition folder different:" + diff;
                                } else {
                                    result = result + " partition folder is same";
                                }
                            } catch (IOException e) {
                                LOG.info(e.getMessage());
                            }
                        } else {
                            result = "skip update partition";
                        }
                        break;
                    case DROP_PARTITION:
                        pDst = this.dstClient.getPartition(db, table, opt);
                        result = pDst != null?"delete partition":"skip delete partition";
                        break;
                    case CHECK_DIRECTORY_TABLE:
                        try {
                            Table srcTab = this.srcClient.getTable(db, table);
                            Table dstTab = this.dstClient.getTable(db, table);
                            if(srcTab != null && dstTab != null) {
                                String srcLocation = srcTab.getSd().getLocation();
                                String dstLocation = dstTab.getSd().getLocation();
                                if (srcLocation == null || dstLocation == null) {
                                    result = result + String.format(" table folder path has null. Src isNull:%b Dst isNull:%b", srcLocation, dstLocation );
                                } else {
                                    diff = hdfsFileChanged(db, table, "", new HdfsPath(srcLocation),
                                            new HdfsPath(dstLocation), this.conf);
                                    if (diff != null) {
                                        result = result + " table folder different:" + diff;
                                    } else {
                                        result = result + " table folder is same";
                                    }
                                }
                            } else {
                                result = "skip update table";
                            }
                        } catch (IOException e) {
                            LOG.info(e.getMessage());
                            result = "exception CHECK_DIRECTORY_TABLE";
                        }
                        break;
                    case CREATE_TABLE_NONPART:
                        Table srcTab = this.srcClient.getTable(db, table);
                        if(srcTab != null) {
                            HdfsPath srcHdfsPath = new HdfsPath(srcTab.getSd().getLocation());
                            if (!allowS3TableSync && srcHdfsPath.getProto().contains("s3")) {
                                result = "table is s3 backed skip";
                            } else {
                                if (!srcHdfsPath.getHost().matches(HDFSPATH_CHECK_MAP.get(this.metastore)) &&
                                        !srcHdfsPath.getProto().contains("s3")) {
                                    result = "table is from different cluster";
                                } else {
                                    result = "copy new directory:" + srcHdfsPath.getFullPath();
                                }
                            }
                        } else {
                            result = "skip add non-partition table";
                        }
                        break;
                    case CREATE_TABLE:
                    case RECREATE_TABLE:
                        // Need to create table metadata in this step for partitioned table.
                        Table srcTable = this.srcClient.getTable(db, table);
                        if (srcTable != null) {
                            if (dstClient.getDatabase(db) == null) {
                                Database srcDb = srcClient.getDatabase(db);

                                HdfsPath dbPath = new HdfsPath(srcDb.getLocationUri());
                                srcDb.setLocationUri(dbPath.replaceHost(NAMENODE_MAP.get(dstMetastore)));
                                dstClient.createDatabase(srcDb);
                            }

                            if (!srcTable.getTableType().contains("VIEW")) {
                                HdfsPath tablePath = new HdfsPath(srcTable.getSd().getLocation());
                                if (!getClusterName(tablePath).equals("s3")) {
                                    srcTable.getSd().setLocation(tablePath.replaceHost(NAMENODE_MAP.get(dstMetastore)));
                                }
                            }

                            if (action == MetastoreAction.RECREATE_TABLE) {
                                Table dstTable = dstClient.getTable(db, table);
                                if (dstTable != null && !dstTable.getTableType().contains("VIEW")) {
                                    HdfsPath dstPath = new HdfsPath(dstTable.getSd().getLocation());
                                    if (!allowS3TableSync && dstPath != null && dstPath.getProto().contains("s3")) {
                                        result = "skip recreate table. Destination is on s3.";
                                        break;
                                    }
                                }

                                // recreate table need to merge the dst parameters into source.
                                mergeParameters(srcTable, dstTable);

                                dstClient.dropTable(db, table, true);

                                result = "table is dropped";
                            }

                            srcTable.putToParameters(REPLICATED_FROM_PINKY_BRAIN, "true");

                            dstClient.createTable(srcTable);
                            result += "table is created";
                            LOG.info("Create table on destination" + db + ":" + table);
                        }
                        break;
                    }
                } catch (HiveMetastoreException e) {
                    LOG.info(String.format("Hit exception during db:%s, tbl:%s, part:%s", db, table, opt));
                    result = String.format("exception in %s of mapper = %s", action.toString(), context.getTaskAttemptID().toString());
                    LOG.info(e.getMessage());
                }

                context.write(value, new Text(result));
                ++this.count;
                if(this.count % 100 == 0) {
                    LOG.info("Processed " + this.count + "partitions");
                }
            }
        }

        protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
            this.srcClient.close();
            this.dstClient.close();
        }
    }

    public static class ProcessTableMapperBase {
        public static final String TMP_REGEX = "tmp.*"; // hard code tmp db and table blacklist
        private HiveMetastoreClient srcClient;
        private HiveMetastoreClient dstClient;
        // list of db and table blacklist.
        private List<String> metastoreBlackList;
        private String metastore;
        private boolean allowS3TableSync;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            try {
                String[] srcHostPart = context.getConfiguration().get(REPLICATION_METASTORE_SRC_HOST).split(":");
                this.srcClient = new ThriftHiveMetastoreClient(srcHostPart[0], Integer.valueOf(srcHostPart[1]).intValue());
                String[] dstHostPart = context.getConfiguration().get(REPLICATION_METASTORE_DST_HOST).split(":");
                this.dstClient = new ThriftHiveMetastoreClient(dstHostPart[0], Integer.valueOf(dstHostPart[1]).intValue());
                this.metastoreBlackList = Arrays.asList(context.getConfiguration().get(REPLICATION_METASTORE_BLACKLIST).split(","));
                this.metastore = context.getConfiguration().get(REPLICATION_METASTORE_COPYFROM, "brain");
                this.allowS3TableSync = context.getConfiguration().getBoolean(REPLICATION_ALLOW_S3, false);
            } catch (HiveMetastoreException e) {
                throw new IOException(e);
            }
        }

        protected List<String> processTable(final String db, final String table)
                throws HiveMetastoreException {
            // If table and db matches black list, we will skip it.
            if (Iterables.any(metastoreBlackList,
                    new Predicate<String>() {
                        @Override
                        public boolean apply(@Nullable String s) {
                            String[] parts = s.split(":");
                            // If db.table match blacklist or db, table match tmp table and db regex we will blacklist it.
                            if ((db.matches(parts[0]) && table.matches(parts[1])) || db.matches(TMP_REGEX) || table.matches(
                                    TMP_REGEX)) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    })) {
                return ImmutableList.of(genValue(MetastoreAction.SKIP_TABLE.name(), db, table, "source blacklisted"));
            }

            Table tab = srcClient.getTable(db, table);
            Table dstTab = dstClient.getTable(db, table);
            if (tab == null) {
                return ImmutableList.of(genValue(MetastoreAction.SKIP_TABLE.name(), db, table, "source deleted"));
            } else {
                String srcLocation = tab.getSd().getLocation();
                if (!tab.getTableType().contains("VIEW")) {
                    HdfsPath ret = new HdfsPath(srcLocation);
                    if (!allowS3TableSync && ret.getProto().contains("s3")) {
                        return ImmutableList.of(
                                genValue(MetastoreAction.SKIP_TABLE.name(), db, table,
                                        "s3 backed table skipped"));
                    } else {
                        if (!ret.getHost().matches(MetastoreReplicationJobV2.HDFSPATH_CHECK_MAP.get(this.metastore)) &&
                                !ret.getProto().contains("s3")) {
                            return ImmutableList.of(
                                    genValue(MetastoreAction.SKIP_TABLE.name(), db, table,
                                            "table is from different cluster"));
                        }
                    }
                }

                ArrayList<String> ret = new ArrayList<>();
                if (dstTab == null) {
                    if (tab.getPartitionKeys().size() > 0) {
                        ret.add(genValue(MetastoreAction.CREATE_TABLE.name(), db, table, tab.getSd().getLocation()));

                        ret.addAll(
                                Lists.transform(srcClient.getPartitionNames(db, table), new Function<String, String>() {
                                    public String apply(String s) {
                                        return genValue(MetastoreAction.CREATE_PARTITION.name(), db, table, s);
                                    }
                                }));
                    } else {
                        if (tab.getTableType().contains("VIEW")) {
                            ret.add(genValue(MetastoreAction.CREATE_TABLE.name(), db, table, ""));
                        } else {
                            ret.add(genValue(MetastoreAction.CREATE_TABLE_NONPART.name(), db, table,
                                    tab.getSd().getLocation()));
                        }
                    }
                } else {
                    UpdateAction action = MetastoreCompareUtils.CompareTableForReplication(tab, dstTab);
                    switch (action) {
                    case UPDATE:
                        ret.add(genValue(MetastoreAction.UPDATE_TABLE.name(), db, table, "table def changed"));
                        break;
                    case RECREATE:
                        // work around for hive bug that can not drop view
                        if (tab.getTableType().contains("VIEW")) {
                            ret.add(genValue(MetastoreAction.UPDATE_TABLE.name(), db, table, "view def changed"));
                        } else {
                            ret.add(genValue(MetastoreAction.RECREATE_TABLE.name(), db, table, "table def changed"));
                        }
                        break;
                    case NO_CHANGE:
                        if (!dstTab.getParameters().containsKey(REPLICATED_FROM_PINKY_BRAIN)) {
                            ret.add(genValue(MetastoreAction.UPDATE_TABLE.name(), db, table, "mark table replicated from pinky/brain"));
                        } else {
                            ret.add(genValue(MetastoreAction.SAME_META_TABLE.name(), db, table,
                                    "table def not changed"));
                        }
                        break;
                    default:
                        throw new HiveMetastoreException(new InterruptedException("invalid table compare result"));
                    }

                    if (tab.getPartitionKeys().size() > 0) {
                        HashSet<String> partNames = Sets.newHashSet(srcClient.getPartitionNames(db, table));
                        HashSet<String> dstPartNames = Sets.newHashSet(dstClient.getPartitionNames(db, table));
                        ret.addAll(Lists.transform(Lists.newArrayList(Sets.difference(partNames, dstPartNames)),
                                new Function<String, String>() {
                                    public String apply(String s) {
                                        return genValue(MetastoreAction.CREATE_PARTITION.name(), db, table, s);
                                    }
                                }));
                        ret.addAll(Lists.transform(Lists.newArrayList(Sets.difference(dstPartNames, partNames)),
                                new Function<String, String>() {
                                    public String apply(String s) {
                                        return genValue(MetastoreAction.DROP_PARTITION.name(), db, table, s);
                                    }
                                }));
                        ret.addAll(Lists.transform(Lists.newArrayList(Sets.intersection(partNames, dstPartNames)),
                                new Function<String, String>() {
                                    public String apply(String s) {
                                        return genValue(MetastoreAction.UPDATE_PARTITION.name(), db, table, s);
                                    }
                                }));
                    } else if (!tab.getTableType().contains("VIEW")) {
                        ret.add(genValue(MetastoreAction.CHECK_DIRECTORY_TABLE.name(), db, table,
                                "check directory for non-partition table"));
                    }
                }
                return ret;
            }
        }

        protected void cleanup() throws IOException, InterruptedException {
            this.srcClient.close();
            this.dstClient.close();
        }
    }

    public static class ProcessTableMapper extends Mapper<Text, Text, LongWritable, Text> {
        private ProcessTableMapperBase worker = new ProcessTableMapperBase();

        protected void setup(Context context) throws IOException, InterruptedException {
            worker.setup(context);
        }

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                for (String result : worker.processTable(key.toString(), value.toString())) {
                    context.write(new LongWritable((long)result.hashCode()), new Text(result));
                }

                LOG.info(String.format("database %s, table %s processed", key.toString(), value.toString()));
            } catch (HiveMetastoreException e) {
                LOG.info(String.format("database %s, table %s got exception", key.toString(), value.toString()));
                LOG.info(e.getMessage());
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            worker.cleanup();
        }
    }

    public static class ProcessTableMapperWithTextInput extends Mapper<LongWritable, Text, LongWritable, Text> {
        private ProcessTableMapperBase worker = new ProcessTableMapperBase();

        protected void setup(Context context) throws IOException, InterruptedException {
            worker.setup(context);
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String [] columns = value.toString().split("\\.");
                if (columns.length != 2) {
                    LOG.info(String.format("invalid input at line %d: %s", key.get(), value.toString()));
                    return;
                }

                for (String result : worker.processTable(columns[0], columns[1])) {
                    context.write(new LongWritable((long)result.hashCode()), new Text(result));
                }

                LOG.info(String.format("database %s, table %s processed", key.toString(), value.toString()));
            } catch (HiveMetastoreException e) {
                LOG.info(String.format("database %s, table %s got exception", key.toString(), value.toString()));
                LOG.info(e.getMessage());
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            worker.cleanup();
        }
    }
}
