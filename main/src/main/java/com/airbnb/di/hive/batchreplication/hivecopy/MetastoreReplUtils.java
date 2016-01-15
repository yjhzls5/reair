package com.airbnb.di.hive.batchreplication.hivecopy;

import com.airbnb.di.hive.replication.DirectoryCopier;
import com.airbnb.di.hive.replication.configuration.Cluster;
import com.airbnb.di.hive.replication.configuration.HardCodedCluster;
import com.airbnb.di.hive.replication.deploy.ConfigurationException;
import com.airbnb.di.hive.replication.deploy.DeployConfigurationKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Field;
import java.net.URI;

import static com.airbnb.di.hive.replication.deploy.ReplicationLauncher.makeURI;

/**
 * Util class for metastore replication
 */
public class MetastoreReplUtils {
    private static final Log LOG = LogFactory.getLog(MetastoreReplUtils.class);

    private MetastoreReplUtils() {
    }

    public static Cluster getCluster(Configuration conf, boolean source) throws ConfigurationException {
        try {
            String testClassName = conf.get(DeployConfigurationKeys.BATCH_JOB_INJECTION_CLASS);

            if (testClassName != null) {
                Class c = Class.forName(testClassName);
                if (source) {
                    Field srcCluster = c.getDeclaredField("srcCluster");
                    srcCluster.setAccessible(true);
                    return (Cluster) srcCluster.get(null);
                } else {
                    Field destCluster = c.getDeclaredField("destCluster");
                    destCluster.setAccessible(true);
                    return (Cluster) destCluster.get(null);
                }
            } else {
                if (source) {
                    String srcClusterName = conf.get(
                            DeployConfigurationKeys.SRC_CLUSTER_NAME);
                    String srcMetastoreUrlString = conf.get(
                            DeployConfigurationKeys.SRC_CLUSTER_METASTORE_URL);
                    URI srcMetastoreUrl = makeURI(srcMetastoreUrlString);
                    String srcHdfsRoot = conf.get(
                            DeployConfigurationKeys.SRC_HDFS_ROOT);
                    String srcHdfsTmp = conf.get(
                            DeployConfigurationKeys.SRC_HDFS_TMP);
                    return new HardCodedCluster(
                            srcClusterName,
                            srcMetastoreUrl.getHost(),
                            srcMetastoreUrl.getPort(),
                            null,
                            null,
                            new Path(srcHdfsRoot),
                            new Path(srcHdfsTmp));
                } else {
                    String destClusterName = conf.get(
                            DeployConfigurationKeys.DEST_CLUSTER_NAME);
                    String destMetastoreUrlString = conf.get(
                            DeployConfigurationKeys.DEST_CLUSTER_METASTORE_URL);
                    URI destMetastoreUrl = makeURI(destMetastoreUrlString);
                    String destHdfsRoot = conf.get(
                            DeployConfigurationKeys.DEST_HDFS_ROOT);
                    String destHdfsTmp = conf.get(
                            DeployConfigurationKeys.DEST_HDFS_TMP);
                    return new HardCodedCluster(
                            destClusterName,
                            destMetastoreUrl.getHost(),
                            destMetastoreUrl.getPort(),
                            null,
                            null,
                            new Path(destHdfsRoot),
                            new Path(destHdfsTmp));
                }
            }
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            LOG.error("Failed to load cluster", e);
            throw new ConfigurationException(e);
        }
    }

    public static DirectoryCopier getDirectoryCopier(Configuration conf) {
        try {
            String testClassName = conf.get(DeployConfigurationKeys.BATCH_JOB_INJECTION_CLASS);

            if (testClassName != null) {
                Class c = Class.forName(testClassName);
                Field directoryCopier = c.getDeclaredField("directoryCopier");
                directoryCopier.setAccessible(true);
                return (DirectoryCopier) directoryCopier.get(null);
            } else {
                String srcHdfsTmp = conf.get(
                        DeployConfigurationKeys.SRC_HDFS_TMP);
                return new DirectoryCopier(conf, new Path(srcHdfsTmp), true);
            }
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            LOG.error("Failed to load cluster", e);
        }
        return null;
    }
}
