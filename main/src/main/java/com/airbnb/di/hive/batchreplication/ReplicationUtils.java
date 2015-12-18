package com.airbnb.di.hive.batchreplication;

import com.airbnb.di.hive.batchreplication.hivecopy.HdfsPath;
import com.google.common.base.Joiner;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class ReplicationUtils {
    private static final Log LOG = LogFactory.getLog(ReplicationUtils.class);

    public ReplicationUtils() {
    }

    public static String doCopyFileAction(Configuration conf, ExtendedFileStatus srcFileStatus, FileSystem srcFs, String dstFolderPath,
                                          FileSystem dstFs, Progressable progressable, boolean forceUpdate, String identifier) {
        int retry = 3;
        String lastError = null;

        while(retry > 0) {
            try {
                Path srcPath = new Path(srcFileStatus.getFullPath());
                if(!srcFs.exists(srcPath)) {
                    LOG.info("Src does not exist. " + srcFileStatus.getFullPath());
                    return "Src does not exist. " + srcFileStatus.getFullPath();
                }
                FileStatus srcStatus = srcFs.getFileStatus(srcPath);

                FSDataInputStream inputStream = srcFs.open(srcPath);
                Path dstPath = new Path(dstFolderPath + "/" + srcFileStatus.getFileName());
                // if dst already exists.
                if(dstFs.exists(dstPath)) {
                    FileStatus dstStatus = dstFs.getFileStatus(dstPath);
                    // If it is not force update, and the file size are same we will not recopy.
                    // This normally happens when we do retry run.
                    if (!forceUpdate && srcStatus.getLen() == dstStatus.getLen()) {
                        LOG.info("dst already exists. " + dstPath.toString());
                        return "dst already exists. " + dstPath.toString();
                    }
                }

                Path dstParentPath = new Path(dstFolderPath);
                if(!dstFs.exists(dstParentPath) && !dstFs.mkdirs(dstParentPath)) {
                    LOG.info("Could not create directory: " + dstFolderPath);
                    return "Could not create directory: " + dstFolderPath;
                }

                Path tmpDstPath = new Path(dstFolderPath + "/__tmp__copy__file_" + identifier + "." + System.currentTimeMillis());
                if(dstFs.exists(tmpDstPath)) {
                    dstFs.delete(tmpDstPath, false);
                }

                FSDataOutputStream outputStream = dstFs.create(tmpDstPath,
                        progressable);
                IOUtils.copyBytes(inputStream, outputStream, conf);
                inputStream.close();
                outputStream.close();
                if(forceUpdate && dstFs.exists(dstPath)) {
                    dstFs.delete(dstPath, false);
                }

                dstFs.rename(tmpDstPath, dstPath);
                dstFs.setTimes(dstPath, srcStatus.getModificationTime(), srcStatus.getAccessTime());
                LOG.info(dstPath.toString() + " file copied");
                progressable.progress();
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                LOG.info(e.getMessage());
                lastError = e.getMessage();
                --retry;
            }
        }

        return lastError;
    }

    public static String genValue(String... columns) {
        return Joiner.on("\t").useForNull("NULL").join(columns);
    }

    public static String getClusterName(HdfsPath path) {
        if(path.getHost().matches("airfs-silver")) {
            return "silver";
        } else if(path.getHost().matches("airfs-brain")) {
            return "brain";
        } else if(path.getHost().matches("airfs-gold")) {
            return "gold";
        } else if(path.getProto().matches("s3n|s3a|s3")) {
            return "s3";
        } else {
            LOG.info("invalid cluster path:" + path.getFullPath());
            return "unknown";
        }
    }

    public static void removeOutputDirectory(String path, Configuration conf) throws IOException {
        Path outputPath = new Path(path);
        FileSystem fs = outputPath.getFileSystem(conf);
        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

}
