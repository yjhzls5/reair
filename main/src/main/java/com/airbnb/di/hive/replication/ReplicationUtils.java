package com.airbnb.di.hive.replication;

import com.airbnb.di.hive.common.HiveObjectSpec;
import com.airbnb.di.hive.common.HiveMetastoreClient;
import com.airbnb.di.hive.common.HiveMetastoreException;
import com.airbnb.di.hive.common.HiveParameterKeys;

import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ReplicationUtils {
  private static final Log LOG = LogFactory.getLog(ReplicationUtils.class);

  // For doing exponential backoff, the number of seconds to use as the base
  private static final int DEFAULT_WAIT_TIME_BASE = 2;
  // For doing exponential backoff, the maximum number of seconds to use
  private static final int DEFAULT_MAX_WAIT_TIME = 3600;

  public static void checkSpecMatch(HiveObjectSpec spec, Table t) {
    if (t != null && (!spec.getDbName().equals(t.getDbName())
        || !spec.getTableName().equals(t.getTableName()))) {
      throw new RuntimeException("Mismatch between spec and Thrift " + "object");
    }
  }

  public static Table stripNonComparables(Table table) {
    Table newTable = new Table(table);
    newTable.setCreateTime(0);
    newTable.setLastAccessTime(0);
    return newTable;
  }

  public static Partition stripNonComparables(Partition partition) {
    Partition newPartition = new Partition(partition);
    newPartition.setCreateTime(0);
    newPartition.setLastAccessTime(0);
    return newPartition;
  }

  public static <T extends TBase> T deserializeObject(String serializedObject, T obj)
      throws MetadataException {
    TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());

    try {
      deserializer.deserialize(obj, serializedObject, "UTF-8");
      return obj;
    } catch (TException e) {
      throw new MetadataException(e);
    }
  }

  public static void createDbIfNecessary(HiveMetastoreClient srcMs, HiveMetastoreClient destMs,
      String dbName) throws HiveMetastoreException {
    if (destMs.existsDb(dbName)) {
      LOG.debug("DB " + dbName + " already exists on destination.");
      return;
    } else {
      Database srcDb = srcMs.getDatabase(dbName);
      if (srcDb == null) {
        LOG.warn(String.format("DB %s doesn't exist on the source!", dbName));
        return;
      }
      Database dbToCreate = new Database(srcDb.getName(), srcDb.getDescription(), null, null);
      LOG.debug("Creating DB: " + dbToCreate);
      destMs.createDatabase(dbToCreate);
    }
  }

  public static boolean exists(HiveMetastoreClient ms, HiveObjectSpec spec)
      throws HiveMetastoreException {
    if (spec.isPartition()) {
      return ms.existsPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());
    } else {
      return ms.existsTable(spec.getDbName(), spec.getTableName());
    }
  }

  public static boolean schemasMatch(Table srcTable, Table destTable) {
    return srcTable.getSd().getCols().equals(destTable.getSd().getCols())
        && srcTable.getPartitionKeys().equals(destTable.getPartitionKeys());
  }

  public static boolean similarEnough(Table srcTable, Table destTable) {
    return ReplicationUtils.stripNonComparables(srcTable)
        .equals(ReplicationUtils.stripNonComparables(destTable));
  }

  public static boolean transientLastDdlTimesMatch(String expectedTldt, Table b) {
    return StringUtils.equals(expectedTldt, b.getParameters().get(HiveParameterKeys.TLDT));
  }

  public static boolean transientLastDdlTimesMatch(String expectedTldt, Partition b) {
    return StringUtils.equals(expectedTldt, b.getParameters().get(HiveParameterKeys.TLDT));
  }

  public static boolean transientLastDdlTimesMatch(Table a, Table b) {
    if (a == null || b == null) {
      return false;
    }

    return StringUtils.equals(a.getParameters().get(HiveParameterKeys.TLDT),
        b.getParameters().get(HiveParameterKeys.TLDT));
  }

  public static boolean transientLastDdlTimesMatch(Partition a, Partition b) {
    if (a == null || b == null) {
      return false;
    }

    return StringUtils.equals(a.getParameters().get(HiveParameterKeys.TLDT),
        b.getParameters().get(HiveParameterKeys.TLDT));
  }

  public static List<String> convertToList(String json) {
    try {
      ObjectMapper om = new ObjectMapper();
      return om.readValue(json, new TypeReference<List<String>>() {});
    } catch (IOException e) {
      return null;
    }
  }

  public static Map<String, String> convertToMap(String json) {
    try {
      ObjectMapper om = new ObjectMapper();
      return om.readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (IOException e) {
      return null;
    }
  }

  public static String convertToJson(List<String> list) throws IOException {
    // writerWithDefaultPrettyPrinter() bundled in with CDH is not present,
    // so using this deprecated method.
    @SuppressWarnings("deprecation")
    ObjectWriter ow = new ObjectMapper().defaultPrettyPrintingWriter();
    return ow.writeValueAsString(list);
  }

  public static String convertToJson(Map<String, String> map) throws IOException {
    // writerWithDefaultPrettyPrinter() bundled in with CDH is not present,
    // so using this deprecated method.
    @SuppressWarnings("deprecation")
    ObjectWriter ow = new ObjectMapper().defaultPrettyPrintingWriter();
    return ow.writeValueAsString(map);
  }

  public static Optional<Path> getLocation(Table table) {
    if (table == null || table.getSd() == null || table.getSd().getLocation() == null) {
      return Optional.empty();
    } else {
      return Optional.ofNullable(new Path(table.getSd().getLocation()));
    }
  }

  public static Optional<Path> getLocation(Partition p) {
    if (p == null || p.getSd() == null || p.getSd().getLocation() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(new Path(p.getSd().getLocation()));
  }

  public static Optional<String> getTldt(Table t) {
    if (t == null || t.getParameters() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(t.getParameters().get(HiveParameterKeys.TLDT));
  }

  public static Optional<String> getTldt(Partition p) {
    if (p == null || p.getParameters() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(p.getParameters().get(HiveParameterKeys.TLDT));
  }

  public static ReplicationStatus toReplicationStatus(RunInfo.RunStatus runStatus) {
    switch (runStatus) {
      case SUCCESSFUL:
        return ReplicationStatus.SUCCESSFUL;
      case NOT_COMPLETABLE:
        return ReplicationStatus.NOT_COMPLETABLE;
      case FAILED:
        return ReplicationStatus.FAILED;
      default:
        throw new RuntimeException("State not handled: " + runStatus);
    }
  }

  public static void sleep(long sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      LOG.error("Unexpectedly interrupted!");
    }
  }

  public static boolean fromSameTable(Collection<Partition> partitions) {
    if (partitions.size() == 0) {
      return false;
    }
    String dbName = null;
    String tableName = null;

    for (Partition p : partitions) {
      if (dbName == null) {
        dbName = p.getDbName();
      } else if (!dbName.equals(p.getDbName())) {
        return false;
      }

      if (tableName == null) {
        tableName = p.getTableName();
      } else if (!tableName.equals(p.getTableName())) {
        return false;
      }
    }
    return true;
  }

  public static Optional<Path> getCommonDirectory(Set<Path> dirs) {
    if (dirs.size() == 0) {
      return Optional.empty();
    }
    // First verify that all the schemes and authorities are the same
    String scheme = null;
    String authority = null;

    for (Path dir : dirs) {
      if (scheme == null) {
        scheme = dir.toUri().getScheme();
      }
      if (authority == null) {
        authority = dir.toUri().getAuthority();
      }

      if (!scheme.equals(dir.toUri().getScheme())) {
        return Optional.empty();
      }

      // Authority can be null - for example: file:///abc/
      if (authority != null && !authority.equals(dir.toUri().getAuthority())) {
        return Optional.empty();
      }
    }

    String commonDir = null;
    for (Path dir : dirs) {
      String dirPathString = dir.toUri().getPath();
      if (commonDir == null) {
        commonDir = dirPathString;
      } else {
        commonDir = commonDir(commonDir, dirPathString);
      }
    }

    return Optional.of(new Path(scheme, authority, commonDir));
  }

  /**
   * @param dir1
   * @param dir2
   * @return the most specific directory that contains both dir1 and dir2. e.g /a/b/c, /a/d/e => /a
   */
  public static String commonDir(String dir1, String dir2) {
    String[] path1Elements = dir1.split("/");
    String[] path2Elements = dir2.split("/");
    List<String> commonPath = new ArrayList<>();

    int i = 0;
    while (i < path1Elements.length && i < path2Elements.length) {
      if (path1Elements[i].equals(path2Elements[i])) {
        commonPath.add(path1Elements[i]);
      } else {
        break;
      }
      i++;
    }
    return org.apache.commons.lang.StringUtils.join(commonPath, "/");
  }

  public static Set<Path> getLocations(Collection<Partition> partitions) {
    Set<Path> paths = new HashSet<>();
    for (Partition p : partitions) {
      String location = p.getSd().getLocation();
      if (location != null) {
        paths.add(new Path(location));
      }
    }
    return paths;
  }

  public static void exponentialSleep(int attempt) throws InterruptedException {
    exponentialSleep(attempt, DEFAULT_WAIT_TIME_BASE, DEFAULT_MAX_WAIT_TIME);
  }

  public static void exponentialSleep(int attempt, int base, int max) throws InterruptedException {
    long sleepSeconds = (long) Math.min(max, Math.pow(base, attempt));
    LOG.debug(String.format("Attempt %d: sleeping for %d seconds", attempt, sleepSeconds));
    Thread.sleep(1000 * sleepSeconds);
  }

  public static boolean equalLocations(Table a, Table b) {
    return StringUtils.equals(getLocation(a).toString(), getLocation(b).toString());
  }

  public static boolean equalLocations(Partition a, Partition b) {
    return StringUtils.equals(getLocation(a).toString(), getLocation(b).toString());
  }
}
