package com.airbnb.di.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.datanucleus.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Utility methods related to the file system
 */
public class FsUtils {

    private static final Log LOG = LogFactory.getLog(FsUtils.class);

    /**
     * @param input string to sanitize
     * @return the same string with non alphanumeric characters replaced by
     * underscores - needed as filesystems don't support some characters well.
     * E.g. : in HDFS
     */
    public static String sanitize(String input) {
        return input.replaceAll("[^\\w\\-=]", "_");
    }


    public static long getSize(Configuration conf, Path p)
            throws IOException {
        return getSize(conf, p, null);
    }
    /**
     * @param conf
     * @param p
     * @param skipPrefix Don't count the sizes of files or directories that
     *                   that start with this prefix
     * @return the size of the given location in bytes, including the size of
     * any subdirectories.
     * @throws java.io.IOException
     */
    public static long getSize(Configuration conf, Path p, String skipPrefix)
            throws IOException {
        long totalSize = 0;
        /*
        // This approach is simpler, but not as memory efficient
        for (FileStatus s : getFileStatusesRecursive(conf, p, skipPrefix)) {
            totalSize += s.getLen();
        }
        return totalSize;
        */
        FileSystem fs = FileSystem.get(p.toUri(), conf);

        Queue<Path> pathsToCheck = new LinkedList<Path>();
        pathsToCheck.add(p);

        // Traverse the directory tree and find all the paths
        // Use this instead of listFiles() as there seems to be more errors
        // related to block locations when using with s3n
        while (pathsToCheck.size() > 0) {
            Path pathToCheck = pathsToCheck.remove();
            if (skipPrefix != null &&
                    pathToCheck.getName().startsWith(skipPrefix)) {
                LOG.warn("Skipping check of directory: " +
                        pathToCheck);
                continue;
            }
            FileStatus[] statuses = fs.listStatus(pathToCheck);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    pathsToCheck.add(status.getPath());
                } else {
                    totalSize += status.getLen();
                }
            }
        }
        return totalSize;
    }

    public static boolean exceedsSize(Configuration conf, Path p, long maxSize)
            throws IOException {
        long totalSize = 0;
        FileSystem fs = FileSystem.get(p.toUri(), conf);

        Queue<Path> pathsToCheck = new LinkedList<Path>();
        pathsToCheck.add(p);

        // Traverse the directory tree and find all the paths
        // Use this instead of listFiles() as there seems to be more errors
        // related to block locations when using with s3n
        while (pathsToCheck.size() > 0) {
            Path pathToCheck = pathsToCheck.remove();
            FileStatus[] statuses = fs.listStatus(pathToCheck);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    pathsToCheck.add(status.getPath());
                } else {
                    totalSize += status.getLen();
                    if (totalSize > maxSize) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     *
     * @param conf
     * @param p
     * @return Fetches the FileStatus of all the files in the supplied path,
     * including subdirectories
     * @throws IOException
     */
    public static Set<FileStatus> getFileStatusesRecursive(Configuration conf,
                                                            Path p,
                                                            String skipPrefix)
            throws IOException {
        FileSystem fs = FileSystem.get(p.toUri(), conf);
        Set<FileStatus> fileStatuses = new HashSet<FileStatus>();

        Queue<Path> pathsToCheck = new LinkedList<Path>();
        pathsToCheck.add(p);

        // Traverse the directory tree and find all the paths
        // Use this instead of listFiles() as there seems to be more errors
        // related to block locations when using with s3n
        while (pathsToCheck.size() > 0) {
            Path pathToCheck = pathsToCheck.remove();
            if (skipPrefix != null &&
                    pathToCheck.getName().startsWith(skipPrefix)) {
                LOG.warn("Skipping check of directory: " +
                        pathToCheck);
                continue;
            }
            FileStatus[] statuses = fs.listStatus(pathToCheck);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    pathsToCheck.add(status.getPath());
                } else {
                    fileStatuses.add(status);
                }
            }
        }
        return fileStatuses;
    }

    /**
     *
     * @param root
     * @param statuses a set of statuses for all the files in the root directory
     * @return a map from the path to a file relative to the root (e.g. a/b.txt)
     * to the associated file size
     */
    private static Map<String, Long> getRelPathToSizes(Path root,
                                                       Set<FileStatus> statuses)
            throws ArgumentException {
        Map<String, Long> pathToStatus = new HashMap<String, Long>();
        for (FileStatus status : statuses) {
            pathToStatus.put(getRelativePath(root,
                    status.getPath()),
                    status.getLen());
        }
        return pathToStatus;
    }

    /**
     *
     * @param root
     * @param statuses a set of statuses for all the files in the root directory
     * @return a map from the path to a file relative to the root (e.g. a/b.txt)
     * to the associated modification time
     */
    private static Map<String, Long> getRelPathToModificationTime(
            Path root,
            Set<FileStatus> statuses)
            throws ArgumentException {
        Map<String, Long> pathToStatus = new HashMap<String, Long>();
        for (FileStatus status : statuses) {
            pathToStatus.put(getRelativePath(root,
                            status.getPath()),
                    status.getModificationTime());
        }
        return pathToStatus;
    }

    /**
     *
     * @param root
     * @param child
     * @return The relative path of the child given the root. For example, if
     * the root was '/a' and the file was '/a/b/c.txt', the relative path would
     * be 'b/c.txt'
     */
    public static String getRelativePath(Path root, Path child) {
        String prefix = root.toString() + "/";
        if (!child.toString().startsWith(prefix)) {
            throw new RuntimeException("Invalid root: " + root +
                    " and child " + child);
        }
        return child.toString().substring(prefix.length());
    }

    /**
     *
     * @param relPathToSize a map from the relative path to a file to the file
     *                      size
     * @return the total size of the files described in the map
     */
    private static long totalSize(Map<String, Long> relPathToSize) {
        long total = 0;
        for (Long l : relPathToSize.values()) {
            total += l;
        }
        return total;
    }

    /**
     * Checks to see if filenames exist on a destination directory that don't
     * exist in the source directory. Mainly used for checking if a distcp
     * -update can work.
     *
     * @param conf
     * @param src
     * @param dest
     * @param skipPrefix files or directories starting with this name are
     *                   excluded when checking the dest directory
     * @return true if there are any file names on the destination directory
     * that are not in the source directory
     * @throws IOException
     */
    public static boolean filesExistOnDestButNotSrc(Configuration conf,
                                                     Path src,
                                                     Path dest,
                                                     String skipPrefix)
            throws IOException {
        Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src,
                skipPrefix);
        Set<FileStatus> destFileStatuses = getFileStatusesRecursive(conf, dest,
                skipPrefix);

        Map<String, Long> srcFileSizes = null;
        Map<String, Long> destFileSizes = null;

        try {
            srcFileSizes = getRelPathToSizes(src, srcFileStatuses);
            destFileSizes = getRelPathToSizes(dest, destFileStatuses);
        } catch (ArgumentException e) {
            throw new IOException("Invalid file statuses!", e);
        }

        for (String file : destFileSizes.keySet()) {
            if (!srcFileSizes.containsKey(file)) {
                LOG.warn(String.format("%s exists on %s but not in %s",
                        file, dest, src));
                return true;
            }
        }
        return false;
    }

    public static boolean equalDirs(Configuration conf, Path src, Path dest)
            throws IOException {
        return equalDirs(conf, src, dest, null);
    }

    /**
     *
     * @param conf
     * @param src
     * @param dest
     * @param skipPrefix files or directories starting with this name are not
     *                   checked
     * @return true if the files in the source and the destination are the
     * 'same'. 'same' is defined as having the same set of files with matching
     * sizes.
     * @throws IOException
     */
    public static boolean equalDirs(Configuration conf, Path src, Path dest,
                                    String skipPrefix)
            throws IOException {
        return equalDirs(conf, src, dest, skipPrefix, false);
    }

    public static boolean equalDirs(Configuration conf, Path src, Path dest,
                                    String skipPrefix,
                                    boolean compareModificationTimes)
            throws IOException {
        boolean srcExists = src.getFileSystem(conf).exists(src);
        boolean destExists = dest.getFileSystem(conf).exists(dest);

        if (!srcExists || !destExists) {
            return false;
        }

        Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src,
                skipPrefix);
        Set<FileStatus> destFileStatuses = getFileStatusesRecursive(conf, dest,
                skipPrefix);

        Map<String, Long> srcFileSizes = null;
        Map<String, Long> destFileSizes = null;

        try {
            srcFileSizes = getRelPathToSizes(src, srcFileStatuses);
            destFileSizes = getRelPathToSizes(dest, destFileStatuses);
        } catch (ArgumentException e) {
            throw new IOException("Invalid file statuses!", e);
        }

        long srcSize = totalSize(srcFileSizes);
        long destSize = totalSize(destFileSizes);

        // Size check is sort of redundant, but is a quick one to show.
        LOG.info("Size of " + src + " is " + srcSize);
        LOG.info("Size of " + dest + " is " + destSize);

        if (srcSize != destSize) {
            LOG.warn(String.format("Size of %s and %s do not match!",
                    src, dest));
            return false;
        }

        if (srcFileSizes.size() != destFileSizes.size()) {
            LOG.warn(String.format("Number of files in %s (%d) and %s (%d) " +
                            "do not match!", src, srcFileSizes.size(), dest,
                    destFileSizes.size()));
            return false;
        }

        for (String file : srcFileSizes.keySet()) {
            if (!destFileSizes.containsKey(file)) {
                LOG.warn(String.format("%s missing from %s!", file, dest));
                return false;
            }
            if (!srcFileSizes.get(file).equals(destFileSizes.get(file))) {
                LOG.warn(String.format("Size mismatch between %s (%d) in %s " +
                                "and %s (%d) in %s", file, srcFileSizes.get(file), src,
                        file, destFileSizes.get(file), dest));
                return false;
            }
        }

        if (compareModificationTimes) {
            Map<String, Long> srcFileModificationTimes = null;
            Map<String, Long> destFileModificationTimes = null;

            try {
                srcFileModificationTimes = getRelPathToModificationTime(src,
                        srcFileStatuses);
                destFileModificationTimes = getRelPathToModificationTime(dest,
                        destFileStatuses);
            } catch (ArgumentException e) {
                throw new IOException("Invalid file statuses!", e);
            }

            for (String file : srcFileModificationTimes.keySet()) {
                if (!srcFileModificationTimes.get(file).equals(
                        destFileModificationTimes.get(file))) {
                    LOG.warn(String.format("Modification time mismatch between " +
                            "%s (%d) in %s and %s (%d) in %s",
                            file, srcFileModificationTimes.get(file), src,
                            file, destFileModificationTimes.get(file), dest));
                    return false;
                }
            }
        }

        LOG.info(String.format("%s and %s are the same", src, dest));
        return true;
    }

    public static void syncModificationTimes(Configuration conf,
                                             Path src, Path dest,
                                             String skipPrefix)
            throws IOException {
        Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src,
                skipPrefix);

        Map<String, Long> srcFileModificationTimes = null;

        try {
            srcFileModificationTimes = getRelPathToModificationTime(src,
                    srcFileStatuses);
        } catch (ArgumentException e) {
            throw new IOException("Invalid file statuses!", e);
        }

        FileSystem destFs = dest.getFileSystem(conf);

        for (String file : srcFileModificationTimes.keySet()) {
            destFs.setTimes(new Path(dest, file),
                    srcFileModificationTimes.get(file),
                    -1);
        }
    }

    /**
     * Moves the directory from the src to dest, creating the parent directory
     * for the dest if one does not exist.
     * @param conf
     * @param src
     * @param dest
     * @throws IOException
     */
    public static void moveDir(Configuration conf, Path src, Path dest)
            throws IOException {
        FileSystem srcFs = FileSystem.get(src.toUri(), conf);
        FileSystem destFs = FileSystem.get(dest.toUri(), conf);
        if (!srcFs.getUri().equals(destFs.getUri())) {
            throw new IOException("Source and destination filesystems " +
                    "are different! src: " + srcFs.getUri() + " dest: " +
                    destFs.getUri());
        }

        Path destPathParent = dest.getParent();
        if (destFs.exists(destPathParent)) {
            if (!destFs.isDirectory(destPathParent)) {
                throw new IOException("File exists instead of destination " +
                        destPathParent);
            } else {
                LOG.info("Parent directory exists: " + destPathParent);
            }
        } else {
            // Parent directory doesn't exist. Create a dummy file to create
            // the "directory"
            destFs.mkdirs(destPathParent);
            //Path fileToTouch = new Path(destPathParent, "$folder$");
            //LOG.info("Creating " + fileToTouch);
            //touchFile(destFs, fileToTouch);
        }
        boolean successful = srcFs.rename(src, dest);
        if (!successful) {
            throw new IOException("Error while moving from " + src
                    + " to " + dest);
        }
    }

    /**
     *
     * @param conf
     * @param p
     * @return true if the path specifies a directory that exists
     * @throws IOException
     */
    public static boolean dirExists(Configuration conf, Path p)
            throws IOException {
        FileSystem fs = FileSystem.get(p.toUri(), conf);
        boolean seemsToExist = fs.exists(p) && fs.isDirectory(p);
        if (seemsToExist) {
            try {
                fs.getStatus(p);
                return true;
            } catch (FileNotFoundException e) {
                LOG.warn("Path " + p + " was said to exist, but getStatus() " +
                        "failed! Assuming to not actually exist");
                return false;
            }
        }
        return false;
    }

    /**
     *
     * @param conf
     * @param p
     * @return true if the path specifies a file that exists
     * @throws IOException
     */
    public static boolean fileExists(Configuration conf, Path p)
            throws IOException {
        FileSystem fs = FileSystem.get(p.toUri(), conf);
        return fs.exists(p) && !fs.isDirectory(p);
    }

    public static long oneWaySync(Configuration conf,
                                  Path srcDir,
                                  Path destDir,
                                  boolean atomic,
                                  Path distCpTmpDir,
                                  Path distCpLogDir)
            throws DistCpException, IOException {
        return oneWaySync(conf, srcDir, destDir, atomic, true, distCpTmpDir,
                distCpLogDir,
                conf.get(ConfigurationKeys.SKIP_CHECK_FILE_PREFIX),
                Integer.parseInt(conf.get(
                        ConfigurationKeys.FILESYSTEM_CONSISTENCY_WAIT_TIME)));
    }

    /**
     * Syncs a directory from srcDir to destDir using distcp. It's a one way
     * sync in that at the end of the method, the destination should be
     * identical to the source. It's idempotent in that it does not do a copy if
     * the files are identical on the source and destination. If atomic is
     * specified, data is first copied to a temporary directory and then moved
     * to the destination to avoid having partial or corrupt data in the
     * destination directory. If canDeleteDest is specified, then the
     * destination directory may be deleted to accomplish the sync.
     *
     * @param conf
     * @param srcDir
     * @param destDir
     * @param distCpTmpDir the temporary directory to copy to before moving
     *                     to destDir if doing an atomic copy
     * @param distCpLogDir the directory where distcp should write logs for the
     *                     copy
     * @param skipPrefix files or directories starting with this name are not
     *                   checked for proper copying.
     * @param fsConsistencyWaitTime The number of seconds to wait after doing a
     *                              major FS operation before checking
     *                              filesystem stats. This is mostly to handle
     *                              S3's eventual consistency.
     * @return the number of bytes copied
     * @throws IOException
     * @throws com.airbnb.di.common.DistCpException
     */
    public static long oneWaySync(Configuration conf,
                                  Path srcDir,
                                  Path destDir,
                                  boolean atomic,
                                  boolean canDeleteDest,
                                  Path distCpTmpDir,
                                  Path distCpLogDir,
                                  String skipPrefix,
                                  int fsConsistencyWaitTime)
        throws IOException, DistCpException {
        return oneWaySync(conf,
                srcDir,
                destDir,
                atomic,
                canDeleteDest,
                distCpTmpDir,
                distCpLogDir,
                skipPrefix,
                fsConsistencyWaitTime,
                false);
    }

    public static boolean sameFs(Path p1, Path p2) {
        return StringUtils.areStringsEqual(p1.toUri().getScheme(),
                p2.toUri().getScheme()) &&
                StringUtils.areStringsEqual(p1.toUri().getAuthority(),
                        p2.toUri().getAuthority());
    }

    public static long oneWaySync(Configuration conf,
                                  Path srcDir,
                                  Path destDir,
                                  boolean atomic,
                                  boolean canDeleteDest,
                                  Path distCpTmpDir,
                                  Path distCpLogDir,
                                  String skipPrefix,
                                  int fsConsistencyWaitTime,
                                  boolean syncModificationTimes)
            throws IOException, DistCpException {

        if (Thread.currentThread().isInterrupted()) {
            throw new DistCpException("Current thread has been interrupted");
        }

        boolean destDirExists = FsUtils.dirExists(conf, destDir);
        LOG.info("Dest dir " + destDir + " exists is " +
                destDirExists);

        if (destDirExists &&
                FsUtils.equalDirs(conf,
                        srcDir,
                        destDir,
                        skipPrefix,
                        syncModificationTimes)) {
            LOG.info("Source and destination paths are already equal!");
            return 0;
        }

        boolean useDistcpUpdate = false;
        // Distcp -update can be used for cases where we're not doing an atomic
        // copy and there aren't any files in the destination that are not in
        // the source. If you delete specific files on the destination, it's
        // possible to do distcp update with unique files in the dest. However,
        // that functionality is not yet built out. Instead, this deletes the
        // destination directory and does a fresh copy.
        if (!atomic) {
            useDistcpUpdate = destDirExists &&
                    !FsUtils.filesExistOnDestButNotSrc(conf, srcDir, destDir,
                            null);
            if (useDistcpUpdate) {
                LOG.info("Doing a distcp update from " + srcDir +
                        " to " + destDir);
            }
        }

        if (destDirExists && !canDeleteDest && !useDistcpUpdate) {
            throw new IOException("Destination directory (" +
                    destDir + ") exists, can't use update, and can't " +
                    "overwrite!");
        }

        if (destDirExists && canDeleteDest && !useDistcpUpdate &&!atomic) {
            LOG.info("Unable to use distcp update, so deleting " + destDir +
                    " since it already exists");
            FsUtils.deleteDirectory(conf, destDir);
            sleep(fsConsistencyWaitTime);
        }

        Path distcpDestDir;
        // For atomic moves, copy to a temporary location and then move the
        // directory to the final destination. Note: S3 doesn't support atomic
        // directory moves so don't use this option for S3 destinations.
        if (atomic) {
            distcpDestDir = distCpTmpDir;
        } else {
            distcpDestDir = destDir;
        }

        LOG.info(String.format("Copying %s to %s",
                srcDir, distcpDestDir));


        long srcSize = FsUtils.getSize(conf, srcDir, skipPrefix);
        LOG.info("Source size is: " + srcSize);

        // TODO: make more configurable
        // Use shell to copy
        if (srcSize < 25e6) {
            String [] mkdirArgs = {"-mkdir",
                    "-p",
                    distcpDestDir.getParent().toString()
            };
            String[] copyArgs = {"-cp",
                    srcDir.toString(),
                    distcpDestDir.toString()
            };

            FsShell shell = new FsShell();
            try {
                LOG.info("Using shell to mkdir with args " + Arrays.asList(mkdirArgs));
                ToolRunner.run(shell, mkdirArgs);
                LOG.info("Using shell to copy with args " + Arrays.asList(copyArgs));
                ToolRunner.run(shell, copyArgs);
            } catch (Exception e) {
               throw new DistCpException(e);
            } finally {
                shell.close();
            }
        } else {

            LOG.info("DistCp log dir: " + distCpLogDir);
            LOG.info("DistCp dest dir: " + distcpDestDir);
            LOG.info("DistCp tmp dir: " + distCpTmpDir);
            // Make sure that the tmp dir and the destination directory are on
            // the same schema
            if (!sameFs(distCpTmpDir, distcpDestDir)) {
                throw new DistCpException(
                        String.format("Filesystems do not match for tmp (%s) " +
                                "and destination (%s)",
                                distCpTmpDir,
                                distcpDestDir));
            }

            List<String> distcpArgs = new ArrayList<String>();
            // TODO: Make num mappers more configurable
            distcpArgs.add("-m");
            distcpArgs.add(Long.toString(srcSize / (long)256e6 + 1));
            distcpArgs.add("-log");
            distcpArgs.add(distCpLogDir.toString());
            if (useDistcpUpdate) {
                distcpArgs.add("-update");
            }
            // Preserve user, group, permissions
            distcpArgs.add("-pugp");
            distcpArgs.add(srcDir.toString());
            distcpArgs.add(distcpDestDir.toString());
            LOG.info("Running DistCp with args: " + distcpArgs);
            String poolName = conf.get(ConfigurationKeys.DISTCP_POOL);
            LOG.info("Using pool: " + poolName);

            // For distcp v1
            // conf.set("pool.name", poolName);
            // DistCp distCp = new DistCp(conf);

            // For distcp v2
            conf.set("mapreduce.job.queuename", poolName);
            DistCp distCp = new DistCp();
            distCp.setConf(conf);

            int ret = distCp.run(distcpArgs.toArray(new String[]{}));

            if (ret != 0) {
                throw new DistCpException("Distcp failed");
            }
        }

        // For S3, waiting a little bit for better consistency has been helpful
        // for increasing the copy success rate.
        sleep(fsConsistencyWaitTime);

        if (!FsUtils.equalDirs(conf,
                srcDir,
                distcpDestDir,
                skipPrefix)) {
            LOG.error("Source and destination sizes don't match!");
            if (atomic) {
                LOG.info("Since it's an atomic copy, deleting " +
                        distcpDestDir);
                FsUtils.deleteDirectory(conf, distcpDestDir);
                throw new DistCpException("distcp result mismatch");
            }
        } else {
            LOG.info("Size of source and destinations match");
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new DistCpException("Current thread has been interrupted");
        }

        if (syncModificationTimes) {
            FsUtils.syncModificationTimes(conf, srcDir, distCpTmpDir,
                    skipPrefix);
        }

        if (atomic) {
            // Size is good, clear out the final destination directory and
            // replace with the copied version.
            destDirExists = FsUtils.dirExists(conf, destDir);
            if (destDirExists) {
                LOG.info("Deleting existing directory " + destDir);
                FsUtils.deleteDirectory(conf, destDir);
            }
            LOG.info("Moving from " + distCpTmpDir + " to " + destDir);
            FsUtils.moveDir(conf, distcpDestDir, destDir);
        }

        LOG.info("Deleting log directory " + distCpLogDir);
        deleteDirectory(conf, distCpLogDir);

        // TODO: Not necessarily the bytes copied if using -update
        return srcSize;
    }


    /**
     * Delete the specified directory. Use FsShell by default, but use s3cmd
     * to delete directories on S3 when possible. s3cmd use can be controlled
     * by ConfigurationKeys.S3CMD_DELETES_ENABLED
     *
     * @param conf
     * @param p
     * @throws IOException
     */
    public static void deleteDirectory(Configuration conf, Path p)
            throws IOException {
        boolean canUseS3cmd = Boolean.parseBoolean(conf.get(
                ConfigurationKeys.S3CMD_DELETES_ENABLED, "false"));
        try {
            if ("s3n".equals(p.toUri().getScheme()) && canUseS3cmd) {
                int ret = deleteUsingS3cmd(conf, p);
                if (ret != 0) {
                    throw new IOException("Error deleting " + p +
                            " with s3cmd!");
                }
            } else {
                String[] deleteArgs = {"-rm", "-r",
                        p.toString()};
                // At Airbnb, the deployed FsShell has some protections to
                // prevent deleting of important datasets. Hence, we use it
                // instead of directly calling FileSystem.rm
                FsShell shell = new FsShell();
                try {
                    ToolRunner.run(shell, deleteArgs);
                } finally {
                    shell.close();
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static int deleteUsingS3cmd(Configuration conf,
                                         Path p) throws IOException {
        try {
            RetryingProcessRunner s3cmdRunner = new RetryingProcessRunner();

            List<String> args = new ArrayList<String>();
            args.add("s3cmd");
            args.add("del");
            args.add("--recursive");
            String s3cmdConfigFile = conf.get(
                    ConfigurationKeys.S3CMD_CONFIGURATION_FILE, "");
            if (s3cmdConfigFile.length() > 0) {
                args.add("-c");
                args.add(s3cmdConfigFile);
            }
            args.add(p.toString().replace("s3n://", "s3://"));

            RunResult result = s3cmdRunner.run(args);
            return result.getReturnCode();
        } catch (ProcessRunException e) {
            throw new IOException(e);
        }
    }

    /**
     * Reads a (small) text file into a string.
     * @param conf
     * @param path
     * @return a single string representing the contents of the file at path
     * @throws IOException
     */
    public static String readSmallFile(Configuration conf, Path path)
            throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataInputStream inputStream = fs.open(path);
        return readIntoString(inputStream);
    }

    /**
     * Given an input stream with text, read into a single string.
     * @param inputStream
     * @return a single string representing the contents of the stream
     * @throws IOException
     */
    private static String readIntoString(FSDataInputStream inputStream)
            throws IOException {
        // TODO: Should be able to read >BUFFER_SIZE
        // Leaving as is since this is only used to read metadata files that are
        // very small.
        final int BUFFER_SIZE = 1048576;
        byte [] bytes = new byte[BUFFER_SIZE];
        int bytesRead = inputStream.read(bytes);
        if (bytesRead >= BUFFER_SIZE) {
            throw new RuntimeException("Buffer size " + BUFFER_SIZE +
                    " is too small!");
        }
        return new String(bytes, "UTF-8");
    }

    public static boolean mkdirs(Configuration conf, Path p)
            throws  IOException {
        FileSystem fs = FileSystem.get(p.toUri(), conf);
        return fs.mkdirs(p);
    }

    private static void sleep(int seconds) {
        try {
            LOG.info("Sleeping for " + seconds + " seconds");
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            LOG.error("Interrupted!");
        }
    }

    /**
     * @param conf
     * @param p
     * @param maxDepth The maximum depth to search for subdirectories
     * @return a set of subdirectories that, when combined together, is the same
     * as p
     * @throws IOException
     */
    public static  Set<Path> getConstituentSubdirs(Configuration conf,
                                                   Path p,
                                                   int maxDepth)
            throws IOException {
        return getConstituentSubdirsHelper(conf, p, 0, maxDepth);
    }

    /**
     * Recursive function that's called on each directory while traversing a
     * directory tree to figure out a set of subdirectories that make up a
     * directory. maxDepth is how deep to search, while the currentDepth is the
     * depth of the current directory.
     *
     * @param conf
     * @param p
     * @param currentDepth
     * @param maxDepth
     * @return
     * @throws IOException
     */
    private static Set<Path> getConstituentSubdirsHelper(Configuration conf,
                                                         Path p,
                                                         int currentDepth,
                                                         int maxDepth)
            throws IOException {
        if (currentDepth > maxDepth) {
            throw new RuntimeException("maxDepth > currentDepth");
        }

        Set<Path> subDirectories = new HashSet<Path>();

        if (currentDepth == maxDepth) {
            // If we're not searching any further, we have to add this whole
            // directory to get everything
            subDirectories.add(p);
            return subDirectories;
        }

        FileSystem fs = FileSystem.get(p.toUri(), conf);
        FileStatus[] statuses = fs.listStatus(p);

        // Nothing in this directory so no need to go any further
        if (statuses.length == 0) {
            subDirectories.add(p);
            return subDirectories;
        }

        for (FileStatus status : statuses) {
            // If a directory has any files, then it's tricky to distribute the
            // copy with multiple distcp's. Just copy the whole directory.
            if (status.isFile()) {
                subDirectories.add(p);
                return subDirectories;
            }
        }

        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                // Recursive case, find all subdirectories within
                subDirectories.addAll(getConstituentSubdirsHelper(
                        conf,
                        status.getPath(),
                        currentDepth + 1,
                        maxDepth
                ));
            } else {
                throw new RuntimeException("Shouldn't happen!");
            }
        }
        return subDirectories;
    }

    /**
     * @param p1
     * @param p2
     * @return true if p2 is a subdirectory of p1
     */
    public static boolean isSubDirectory(Path p1, Path p2) {
        return p2.toString().startsWith(p1.toString() + "/");
    }

    public static void replaceDirectory(Configuration conf, Path src,
                                        Path dest) throws IOException {
        FileSystem fs = dest.getFileSystem(conf);
        if (fs.exists(dest)) {
            LOG.info("Removing " + dest + " since it exists");
            deleteDirectory(conf, dest);
        }
        LOG.info("Renaming " + src + " to " + dest);
        fs.rename(src, dest);
    }
}
