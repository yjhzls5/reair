package com.airbnb.di.misc;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;

public class DeserializeTable {

  public static void main(String[] argv) throws Exception {
    String json = argv[0];
    TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
    Table t = new Table();
    deserializer.deserialize(t, json, "UTF-8");
    System.out.println("Object is " + t);
  }
}
