package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.batchreplication.SimpleFileStatus;
import com.airbnb.di.hive.replication.ReplicationUtils;

import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.ClusterFactory;
import com.airbnb.di.hive.replication.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Stage 2 reducer to handle directory copy.
 *
 * <p>Input is the files needs to be copied. Load balance is done through shuffle. Output of the job
 * is file copied or skipped.
 */
public class Stage2DirectoryCopyReducer extends Reducer<LongWritable, Text, Text, Text> {
  private static final Log LOG = LogFactory.getLog(Stage2DirectoryCopyReducer.class);
  private Configuration conf;
  private Cluster dstCluster;


  enum CopyStatus {
    COPIED,
    SKIPPED
  }

  public Stage2DirectoryCopyReducer() {
  }

  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      this.conf = context.getConfiguration();
      ClusterFactory clusterFactory = MetastoreReplUtils.createClusterFactory(conf);
      this.dstCluster = clusterFactory.getDestCluster();
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }
  }

  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    for (Text value : values) {
      String[] fields = value.toString().split("\t");
      String srcFileName = fields[0];
      String dstDirectory = fields[1];
      long size = Long.valueOf(fields[2]);
      SimpleFileStatus fileStatus = new SimpleFileStatus(srcFileName, size, 0L);
      FileSystem srcFs = (new Path(srcFileName)).getFileSystem(this.conf);
      FileSystem dstFs = (new Path(dstDirectory)).getFileSystem(this.conf);
      String result = ReplicationUtils.doCopyFileAction(
          conf,
          fileStatus,
          srcFs,
          dstDirectory,
          dstFs,
          dstCluster.getTmpDir(),
          context,
          false,
          context.getTaskAttemptID().toString());
      if (result == null) {
        context.write(new Text(CopyStatus.COPIED.toString()),
            new Text(ReplicationUtils.genValue(value.toString(), " ",
                String.valueOf(System.currentTimeMillis()))));
      } else {
        context.write(
            new Text(CopyStatus.SKIPPED.toString()),
            new Text(ReplicationUtils.genValue(
                value.toString(),
                result,
                String.valueOf(System.currentTimeMillis()))));
      }
    }
  }
}
