package com.airbnb.di.hive.common;

import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Composite class that combines the Hive Partition thrift object with the associated name.
 */
public class NamedPartition {
  private String name;
  private Partition partition;

  public NamedPartition(NamedPartition o) {
    this.name = o.name;
    this.partition = new Partition(o.partition);
  }

  public NamedPartition(String name, Partition partition) {
    this.name = name;
    this.partition = partition;
  }

  public String getName() {
    return name;
  }

  public Partition getPartition() {
    return partition;
  }

  public static List<Partition> toPartitions(Collection<NamedPartition> collection) {
    List<Partition> partitions = new ArrayList<>();
    for (NamedPartition pwn : collection) {
      partitions.add(pwn.getPartition());
    }
    return partitions;
  }

  public static List<String> toNames(Collection<NamedPartition> collection) {
    List<String> partitionNames = new ArrayList<>();
    for (NamedPartition pwn : collection) {
      partitionNames.add(pwn.getName());
    }
    return partitionNames;
  }
}
