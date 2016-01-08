package com.airbnb.di.hive.replication.primitives;

import org.apache.hadoop.fs.Path;

/**
 * Stores information about the estimated task required to replicate a Hive
 * object.
 */
public class TaskEstimate {
    public enum TaskType {
        COPY_UNPARTITIONED_TABLE,
        COPY_PARTITIONED_TABLE,
        COPY_PARTITION,
        DROP_TABLE,
        DROP_PARTITION,
        NO_OP,
    }

    private TaskType taskType;
    private boolean updateMetadata;
    private boolean updateData;
    private Path srcPath;
    private Path destPath;

    public TaskEstimate(TaskType taskType,
                        boolean updateMetadata,
                        boolean updateData,
                        Path srcPath,
                        Path destPath) {
        this.taskType = taskType;
        this.updateMetadata = updateMetadata;
        this.updateData = updateData;
        this.srcPath = srcPath;
        this.destPath = destPath;
    }

    public boolean isUpdateMetadata() {
        return updateMetadata;
    }

    public boolean isUpdateData() {
        return updateData;
    }

    public Path getSrcPath() {
        return srcPath;
    }

    public Path getDestPath() {
        return destPath;
    }

    public TaskType getTaskType() {
        return taskType;
    }
}
