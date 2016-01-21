package com.airbnb.di.misc;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TJSONProtocol;

public class DeserializePartition {

  public static void main(String[] argv) throws Exception {
    String json = argv[0];
    TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());

    Partition p = new Partition();
    deserializer.deserialize(p, json, "UTF-8");
    System.out.println("Object is " + p);
  }
}
