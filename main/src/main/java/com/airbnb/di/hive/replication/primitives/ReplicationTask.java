package com.airbnb.di.hive.replication.primitives;

import com.airbnb.di.common.DistCpException;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.replication.RunInfo;
import com.airbnb.di.multiprocessing.LockSet;

import java.io.IOException;

public interface ReplicationTask {
    public RunInfo runTask() throws HiveMetastoreException,
            IOException, DistCpException;

    public LockSet getRequiredLocks();


}
