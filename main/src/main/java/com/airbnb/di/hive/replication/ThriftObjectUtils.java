package com.airbnb.di.hive.replication;

import com.airbnb.di.hive.replication.PersistedJobInfo;
import com.airbnb.di.hive.replication.ReplicationJob;
import com.airbnb.di.hive.replication.ReplicationOperation;
import com.airbnb.di.hive.replication.ReplicationStatus;
import com.airbnb.di.hive.replication.thrift.TReplicationJob;
import com.airbnb.di.hive.replication.thrift.TReplicationOperation;
import com.airbnb.di.hive.replication.thrift.TReplicationStatus;

import java.util.ArrayList;
import java.util.List;

public class ThriftObjectUtils {
    private static TReplicationOperation convert(ReplicationOperation op) {
        switch (op) {
            case COPY_UNPARTITIONED_TABLE:
                return TReplicationOperation.COPY_UNPARTITIONED_TABLE;
            case COPY_PARTITIONED_TABLE:
                return TReplicationOperation.COPY_PARTITIONED_TABLE;
            case COPY_PARTITION:
                return TReplicationOperation.COPY_PARTITION;
            case COPY_PARTITIONS:
                return TReplicationOperation.COPY_PARTITIONS;
            case DROP_TABLE:
                return TReplicationOperation.DROP_TABLE;
            case DROP_PARTITION:
                return TReplicationOperation.DROP_PARTITION;
            case RENAME_TABLE:
                return TReplicationOperation.RENAME_TABLE;
            case RENAME_PARTITION:
                return TReplicationOperation.RENAME_PARTITION;
            default:
                throw new RuntimeException("Unhandled operation: " + op);
        }
    }

    private static TReplicationStatus convert(ReplicationStatus status) {
        switch(status) {
            case PENDING:
                return TReplicationStatus.PENDING;
            case RUNNING:
                return TReplicationStatus.RUNNING;
            case SUCCESSFUL:
                return TReplicationStatus.SUCCESSFUL;
            case FAILED:
                return TReplicationStatus.FAILED;
            case NOT_COMPLETABLE:
                return TReplicationStatus.NOT_COMPLETABLE;
            default:
                throw new RuntimeException("Unhandled case: " + status);
        }
    }

    public static TReplicationJob convert(ReplicationJob job) {
        PersistedJobInfo jobInfo = job.getPersistedJobInfo();
        List<Long> parentJobIds = new ArrayList<Long>(job.getParentJobIds());

        return new TReplicationJob(
                job.getId(),
                job.getCreateTime(),
                // TODO: Non-zero
                0,
                convert(jobInfo.getOperation()),
                convert(jobInfo.getStatus()),
                jobInfo.getSrcPath() == null ? null :
                        jobInfo.getSrcPath().toString(),
                jobInfo.getSrcClusterName(),
                jobInfo.getSrcDbName(),
                jobInfo.getSrcTableName(),
                jobInfo.getSrcPartitionNames() == null ?
                        new ArrayList<String>() : jobInfo.getSrcPartitionNames(),
                jobInfo.getSrcObjectTldt(),
                jobInfo.getRenameToDb(),
                jobInfo.getRenameToTable(),
                jobInfo.getRenameToPath() == null ? null :
                        jobInfo.getRenameToPath().toString(),
                jobInfo.getExtras(),
                parentJobIds
                );



    }
}
