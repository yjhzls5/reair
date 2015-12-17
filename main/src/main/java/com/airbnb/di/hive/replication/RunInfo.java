package com.airbnb.di.hive.replication;

public class RunInfo {

    public enum RunStatus {
        SUCCESSFUL,
        NOT_COMPLETABLE,
        FAILED,
    }

    private RunStatus runStatus;
    private long bytesCopied;

    public RunStatus getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(RunStatus runStatus) {
        this.runStatus = runStatus;
    }

    public long getBytesCopied() {
        return bytesCopied;
    }

    public void setBytesCopied(long bytesCopied) {
        this.bytesCopied = bytesCopied;
    }

    public RunInfo(RunStatus runStatus, long bytesCopied) {
        this.runStatus = runStatus;
        this.bytesCopied = bytesCopied;
    }
}
