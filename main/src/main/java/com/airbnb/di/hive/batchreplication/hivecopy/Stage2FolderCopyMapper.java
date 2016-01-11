package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.replication.primitives.TaskEstimate;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.genValue;
import static com.airbnb.di.hive.batchreplication.hivecopy.MetastoreReplicationJob.deseralizeJobResult;

/**
 * Stage 2 Mapper to handle folder hdfs folder copy
 */
public class Stage2FolderCopyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(Stage2FolderCopyMapper.class);
    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    private Configuration conf;

    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Pair<TaskEstimate, HiveObjectSpec> input = deseralizeJobResult(value.toString());
        TaskEstimate estimate = input.getLeft();
        HiveObjectSpec spec = input.getRight();

        switch (estimate.getTaskType()) {
        case COPY_PARTITION:
        case COPY_UNPARTITIONED_TABLE:
            UpdateDirectory(context, spec.getDbName(), spec.getTableName(), spec.getPartitionName(), estimate.getSrcPath(),
                    estimate.getDestPath());
            break;
        default:
            break;

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

    private void UpdateDirectory(Context context, String db, String table, String partition, Path src, Path dst)
            throws IOException, InterruptedException {
        if (!hdfsCleanFolder(db, table, partition, dst.toString(), this.conf, true)) {
            throw new InterruptedException("Failed to update directory:" + dst);
        } else {
            try {
                FileSystem srcFs = src.getFileSystem(this.conf);
                for (FileStatus status : srcFs.listStatus(src, hiddenFileFilter)) {
                    long hashValue = Hashing.murmur3_128().hashLong(
                            (long) (Long.valueOf(status.getLen()).hashCode() *
                                    Long.valueOf(status.getModificationTime()).hashCode())).asLong();
                    context.write(new LongWritable(hashValue), new Text(
                            genValue(status.getPath().toString(), dst.toString(), String.valueOf(status.getLen()))));
                }
            } catch (IOException e) {
                // Ignore File list generate error because source directory could be removed while we
                // enumerate it.
                LOG.info("Src dir is removed:" + src);
            }
        }
    }
}
