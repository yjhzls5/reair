package com.airbnb.reair.batch.hive;

import static com.airbnb.reair.batch.hive.MetastoreReplicationJob.deseralizeJobResult;

import com.google.common.hash.Hashing;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.primitives.TaskEstimate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Stage 2 Mapper to handle copying of HDFS directories.
 *
 * <p>Input of this job is the output of stage 1. It contains the actions to take for the tables and
 * partitions. In this stage, we only care about the COPY actions. In the mapper, it will enumerate
 * the directories and figure out files needs to be copied. Since each directory can have an uneven
 * number of files, we shuffle again to distribute the work for copying files, which is done on the
 * reducers.
 */
public class Stage2DirectoryCopyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(Stage2DirectoryCopyMapper.class);
  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private Configuration conf;

  protected void setup(Context context) throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
  }

  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    Pair<TaskEstimate, HiveObjectSpec> input = deseralizeJobResult(value.toString());
    TaskEstimate estimate = input.getLeft();
    HiveObjectSpec spec = input.getRight();


    switch (estimate.getTaskType()) {
      case COPY_PARTITION:
      case COPY_UNPARTITIONED_TABLE:
        if (estimate.isUpdateData()) {
          updateDirectory(context, spec.getDbName(), spec.getTableName(), spec.getPartitionName(),
              estimate.getSrcPath().get(), estimate.getDestPath().get());
        }
        break;
      default:
        break;

    }
  }

  private static void hdfsCleanDirectory(
      String db,
      String table,
      String part,
      String dst,
      Configuration conf,
      boolean recreate) throws IOException {
    Path dstPath = new Path(dst);

    FileSystem fs = dstPath.getFileSystem(conf);
    if (fs.exists(dstPath) && !fs.delete(dstPath, true)) {
      throw new IOException("Failed to delete destination directory: " + dstPath.toString());
    }

    if (fs.exists(dstPath)) {
      throw new IOException("Validate delete destination directory failed: " + dstPath.toString());
    }

    if (!recreate) {
      return;
    }

    // TODO: 2019/10/10 config perssion
    if (!fs.mkdirs(dstPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))) {
      throw new IOException("Validate recreate destination directory failed: "
          + dstPath.toString());
    }else {
      LOG.info("2 hdfs mkdirs dstPath: "+ dstPath );
    }

  }

  private void updateDirectory(
      Context context,
      String db,
      String table,
      String partition,
      Path src,
      Path dst) throws IOException, InterruptedException {

    LOG.info("updateDirectory:" + dst.toString());

    hdfsCleanDirectory(db, table, partition, dst.toString(), this.conf, true);

    try {
      FileSystem srcFs = src.getFileSystem(this.conf);
      LOG.info("src file: " + src.toString());



      //只拷贝有子文件夹的分区
        FileStatus[] fileStatuses =  handleSubDirectory(src);

    //for (FileStatus status : srcFs.listStatus(src, hiddenFileFilter)) {
      for (FileStatus status : fileStatuses) {
        LOG.info("file: " + status.getPath().toString());

        long hashValue = Hashing.murmur3_128().hashLong(
            (long) (Long.valueOf(status.getLen()).hashCode()
              * Long.valueOf(status.getModificationTime()).hashCode())).asLong();

        context.write(
            new LongWritable(hashValue),
            new Text(ReplicationUtils.genValue(
                    status.getPath().toString(),
                    dst.toString(),
                    String.valueOf(status.getLen()))));
      }
    } catch (IOException e) {
      // Ignore File list generate error because source directory could be removed while we
      // enumerate it.
      LOG.warn("Error listing " + src, e);
    }
  }

  private FileStatus[] handleSubDirectory(
                                  Path src) throws IOException, InterruptedException{

      FileSystem srcFs = src.getFileSystem(this.conf);
      FileStatus[] fsArray = srcFs.listStatus(src,hiddenFileFilter);
      boolean isDirectory = false;
      for (FileStatus f:fsArray) {
        if(f.isDirectory()) {
          isDirectory = true;
          break;
        }
      }

      if(!isDirectory){
        return new FileStatus[0];
      }

      RemoteIterator iterator = srcFs.listFiles(src,true);
      LocatedFileStatus locatedFileStatus = null;
      List<LocatedFileStatus> list = new ArrayList<>();
      while (iterator.hasNext()){
        locatedFileStatus = (LocatedFileStatus)iterator.next();

        LOG.info("handleSubDirectory,"+locatedFileStatus.getPath());

        list.add(locatedFileStatus);

      }
      FileStatus[] fileStatuses = new FileStatus[list.size()];
      int i = -1;
      for (FileStatus fileStatus: list) {
        fileStatuses[++i] = fileStatus;
      }

     return fileStatuses;
  }

}
