package com.airbnb.di.hive.replication.primitives;


public class CopyPartitionsCounter {
  long completionCount = 0;
  long bytesCopiedCount = 0;

  synchronized void incrementCompletionCount() {
    completionCount++;
  }

  synchronized long getCompletionCount() {
    return completionCount;
  }

  synchronized void incrementBytesCopied(long bytesCopied) {
    bytesCopiedCount += bytesCopied;
  }

  synchronized long getBytesCopied() {
    return bytesCopiedCount;
  }
}
