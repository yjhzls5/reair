package com.airbnb.di.hive.batchreplication.hivecopy;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Util class for metastore compare
 */
public class MetastoreCompareUtils {
    private static final Log LOG = LogFactory.getLog(MetastoreCompareUtils.class);

    private MetastoreCompareUtils() {
    }

    public enum UpdateAction {
        UPDATE,
        RECREATE,
        NO_CHANGE
    }

    /**
     * Check property needs recreate table first. Recreate has highest priority.
     * @param src
     * @param dst
     * @return
     */
    private static UpdateAction compareSDForReplication(StorageDescriptor src, StorageDescriptor dst) {
        if (!Iterables.elementsEqual(src.getCols(), dst.getCols())) {
            LOG.info("Cols diff:" + src.getCols() + ":" + dst.getCols());
            return UpdateAction.RECREATE;
        }

        if (src.getNumBuckets() != dst.getNumBuckets()) {
            LOG.info("NumBuckets diff:" + src.getNumBuckets() + ":" + dst.getNumBuckets());
            return UpdateAction.RECREATE;
        }

        if (!Iterables.elementsEqual(src.getBucketCols(), dst.getBucketCols())) {
            LOG.info("Bucket Col diff:" + src.getBucketCols() + ":" + dst.getBucketCols());
            return UpdateAction.RECREATE;
        }

        if (!Iterables.elementsEqual(src.getSortCols(), dst.getSortCols())) {
            LOG.info("Sort Col diff:" + src.getSortCols() + ":" + dst.getSortCols());
            return UpdateAction.RECREATE;
        }

        String src_path = null;
        if (src.getLocation() != null) {
            src_path = new HdfsPath(src.getLocation()).getPath();
        }

        String dst_path = null;
        if (dst.getLocation() != null) {
            dst_path = new HdfsPath(dst.getLocation()).getPath();
        }

        if (ObjectUtils.compare(src_path, dst_path) != 0) {
            LOG.info("Location diff:" + src_path + ":" + dst_path);
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getInputFormat(), dst.getInputFormat()) != 0) {
            LOG.info("InputFormat diff:" + src.getInputFormat() + ":" + dst.getInputFormat());
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getOutputFormat(), dst.getOutputFormat()) != 0) {
            LOG.info("OutputFormat diff:" + src.getOutputFormat() + ":" + dst.getOutputFormat());
            return UpdateAction.UPDATE;
        }

        if (src.isCompressed() != dst.isCompressed()) {
            LOG.info("Compressed diff:" + src.isCompressed() + ":" + dst.isCompressed());
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getSerdeInfo(), dst.getSerdeInfo()) != 0) {
            LOG.info("SerdeInfo diff:" + src.getSerdeInfo() + ":" + dst.getSerdeInfo());
            return UpdateAction.UPDATE;
        }

        if (!Maps.difference(src.getParameters(), dst.getParameters()).areEqual()) {
            LOG.info("Parameters diff:" + src.getParameters() + ":" + dst.getParameters());
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getSkewedInfo(), dst.getSkewedInfo()) != 0) {
            LOG.info("SkewedInfo diff:" + src.getSkewedInfo() + ":" + dst.getSkewedInfo());
            return UpdateAction.UPDATE;
        }

        if (src.isStoredAsSubDirectories() != dst.isStoredAsSubDirectories()) {
            LOG.info("StoredAsSubDirectoies diff:" + src.isStoredAsSubDirectories() + ":" + dst.isStoredAsSubDirectories());
            return UpdateAction.UPDATE;
        }

        return UpdateAction.NO_CHANGE;
    }

    /**
     * Check Recreate change first.
     *
     * @param src
     * @param dst
     * @return
     */
    public static UpdateAction CompareTableForReplication(Table src, Table dst) {
        if (!Iterables.elementsEqual(src.getPartitionKeys(), dst.getPartitionKeys())) {
            LOG.info("PartitionKeys diff:" + src.getPartitionKeys() + ":" + dst.getPartitionKeys());
            return UpdateAction.RECREATE;
        }

        if (ObjectUtils.compare(src.getTableType(), dst.getTableType()) != 0) {
            LOG.info("Table Type diff:" + src.getTableType() + ":" + dst.getTableType());
            return UpdateAction.RECREATE;
        }

        UpdateAction action = compareSDForReplication(src.getSd(), dst.getSd());
        if (action != UpdateAction.NO_CHANGE) {
            return action;
        }

        if (!src.getOwner().equals(dst.getOwner())) {
            LOG.info("ower diff:" + src.getOwner() + ":" + dst.getOwner());
            return UpdateAction.UPDATE;
        }

        if (src.getCreateTime() > dst.getCreateTime()) {
            LOG.info("CTime diff:" + src.getCreateTime() + ":" + dst.getCreateTime());
            return UpdateAction.UPDATE;
        }

        if (src.getRetention() != dst.getRetention()) {
            LOG.info("retention diff:" + src.getRetention() + ":" + dst.getRetention());
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getViewOriginalText(), dst.getViewOriginalText()) != 0) {
            LOG.info("View diff:" + src.getViewOriginalText() + ":" + dst.getViewOriginalText());
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getViewExpandedText(), dst.getViewExpandedText()) != 0) {
            LOG.info("Expanded View Text diff:" + src.getViewExpandedText() + ":" + dst.getViewExpandedText());
            return UpdateAction.UPDATE;
        }

        if (ObjectUtils.compare(src.getPrivileges(), dst.getPrivileges()) != 0) {
            LOG.info("Privileges diff:" + src.getPrivileges() + ":" + dst.getPrivileges());
            return UpdateAction.UPDATE;
        }

        MapDifference<String, String> paraDiff = Maps.difference(src.getParameters(), dst.getParameters());
        if (paraDiff.entriesDiffering().size() > 0 || paraDiff.entriesOnlyOnLeft().size() > 0) {
            LOG.info("Parameters diff:" + paraDiff.entriesDiffering() + ", " + paraDiff.entriesOnlyOnLeft());
            return UpdateAction.UPDATE;
        }

        return UpdateAction.NO_CHANGE;
    }

    public static UpdateAction ComparePartitionForReplication(Partition src, Partition dst) {
        UpdateAction action = compareSDForReplication(src.getSd(), dst.getSd());
        if (action != UpdateAction.NO_CHANGE) {
            return action;
        }

        //create time check will be skipped since we will compare files for sure.

        if (ObjectUtils.compare(src.getPrivileges(), dst.getPrivileges()) != 0) {
            LOG.info("Partition Privileges diff:" + src.getPrivileges() + ":" + dst.getPrivileges());
            return UpdateAction.UPDATE;
        }

        MapDifference<String, String> paraDiff = Maps.difference(src.getParameters(), dst.getParameters());
        if (paraDiff.entriesDiffering().size() > 0 || paraDiff.entriesOnlyOnLeft().size() > 0) {
            LOG.info("Partition Parameters diff:" + paraDiff.entriesDiffering() + ", " + paraDiff.entriesOnlyOnLeft());
            return UpdateAction.UPDATE;
        }

        return UpdateAction.NO_CHANGE;
    }
}
