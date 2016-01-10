package com.airbnb.di.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
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
import java.util.Optional;
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
     * underscores - needed as filesystems don't support all characters well.
     * E.g. : in HDFS
     */
    public static String sanitize(String input) {
        return input.replaceAll("[^\\w\\-=]", "_");
    }

    public static boolean sameFs(Path p1, Path p2) {
        return StringUtils.areStringsEqual(p1.toUri().getScheme(),
                p2.toUri().getScheme()) &&
                StringUtils.areStringsEqual(p1.toUri().getAuthority(),
                        p2.toUri().getAuthority());
    }

    public static long getSize(Configuration conf, Path p)
            throws IOException {
        return getSize(conf, p, Optional.empty());
    }
    /**
     * @param conf
     * @param p
     * @param filter use this to filter out files and directories
     * @return the size of the given location in bytes, including the size of
     * any subdirectories.
     * @throws java.io.IOException
     */
    public static long getSize(Configuration conf,
                               Path p,
                               Optional<PathFilter> filter)
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
            if (filter.isPresent() &&
                    !filter.get().accept(pathToCheck)) {
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
                                                           Optional<PathFilter> filter)
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
            if (filter.isPresent() &&
                    !filter.get().accept(pathToCheck)) {
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
     * @param filter filter to use when traversing through the directories
     * @return true if there are any file names on the destination directory
     * that are not in the source directory
     * @throws IOException
     */
    public static boolean filesExistOnDestButNotSrc(Configuration conf,
                                                    Path src,
                                                    Path dest,
                                                    Optional<PathFilter> filter)
            throws IOException {
        Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src,
                filter);
        Set<FileStatus> destFileStatuses = getFileStatusesRecursive(conf, dest,
                filter);

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
        return equalDirs(conf, src, dest, Optional.empty());
    }

    /**
     *
     * @param conf
     * @param src
     * @param dest
     * @param filter files or directories starting with this name are not
     *                   checked
     * @return true if the files in the source and the destination are the
     * 'same'. 'same' is defined as having the same set of files with matching
     * sizes.
     * @throws IOException
     */
    public static boolean equalDirs(Configuration conf,
                                    Path src,
                                    Path dest,
                                    Optional<PathFilter> filter)
            throws IOException {
        return equalDirs(conf, src, dest, filter, false);
    }

    public static boolean equalDirs(Configuration conf,
                                    Path src,
                                    Path dest,
                                    Optional<PathFilter> filter,
                                    boolean compareModificationTimes)
            throws IOException {
        boolean srcExists = src.getFileSystem(conf).exists(src);
        boolean destExists = dest.getFileSystem(conf).exists(dest);

        if (!srcExists || !destExists) {
            return false;
        }

        Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src,
                filter);
        Set<FileStatus> destFileStatuses = getFileStatusesRecursive(conf, dest,
                filter);

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
        LOG.debug("Size of " + src + " is " + srcSize);
        LOG.debug("Size of " + dest + " is " + destSize);

        if (srcSize != destSize) {
            LOG.debug(String.format("Size of %s and %s do not match!",
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

        LOG.debug(String.format("%s and %s are the same", src, dest));
        return true;
    }

    public static void syncModificationTimes(Configuration conf,
                                             Path src, Path dest,
                                             Optional<PathFilter> filter)
            throws IOException {
        Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src,
                filter);

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
                LOG.debug("Parent directory exists: " + destPathParent);
            }
        } else {
            destFs.mkdirs(destPathParent);
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
        return fs.exists(p) && fs.isDirectory(p);
    }

    /**
     * Delete the specified directory.
     *
     * @throws IOException
     */
    public static void deleteDirectory(Configuration conf, Path p)
            throws IOException {

        Trash trash = new Trash(conf);
        try {
            boolean removed = trash.moveToTrash(p);
            if (removed) {
                LOG.debug("Moved to trash: " + p);
            } else {
                LOG.error("Error moving to trash: " + p);
            }
        } catch (FileNotFoundException e) {
            LOG.debug("Attempting to delete non-existent directory " + p);
            return;
        }
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
            LOG.debug("Removing " + dest + " since it exists");
            deleteDirectory(conf, dest);
        }
        LOG.debug("Renaming " + src + " to " + dest);
        fs.rename(src, dest);
    }
}
