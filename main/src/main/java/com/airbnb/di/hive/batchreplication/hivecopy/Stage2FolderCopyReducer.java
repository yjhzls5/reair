package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.batchreplication.ExtendedFileStatus;
import com.airbnb.di.hive.batchreplication.ReplicationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.airbnb.di.hive.batchreplication.ReplicationUtils.genValue;

/**
 * Stage 2 reducer to handle folder copy.
 */
public class Stage2FolderCopyReducer extends Reducer<LongWritable, Text, Text, Text> {
    private Configuration conf;

    public Stage2FolderCopyReducer() {
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
    }

    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            String srcFileName = fields[0];
            String dstFolder = fields[1];
            long size = Long.valueOf(fields[2]);
            ExtendedFileStatus fileStatus = new ExtendedFileStatus(srcFileName, size, 0L);
            FileSystem srcFs = (new Path(srcFileName)).getFileSystem(this.conf);
            FileSystem dstFs = (new Path(dstFolder)).getFileSystem(this.conf);
            String result = ReplicationUtils.doCopyFileAction(conf, fileStatus, srcFs, dstFolder, dstFs, context, false,
                    context.getTaskAttemptID().toString());
            if (result == null) {
                context.write(new Text("copied"),
                        new Text(genValue(value.toString(), " ", String.valueOf(System.currentTimeMillis()))));
            } else {
                context.write(new Text("skip copy"),
                        new Text(genValue(value.toString(), result, String.valueOf(System.currentTimeMillis()))));
            }
        }
    }
}
