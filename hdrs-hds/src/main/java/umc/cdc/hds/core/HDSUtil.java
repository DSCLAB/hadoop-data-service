package umc.cdc.hds.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import umc.cdc.hds.datastorage.status.DataRecord;

/**
 *
 * @author jpopaholic
 */
public class HDSUtil {

  private static final Log LOG = LogFactory.getLog(HDSUtil.class);

  public static Configuration CONFIG = loadCustomHdsConfiguration();

  public static Configuration loadCustomHdsConfiguration() {
    Configuration config = HBaseConfiguration.create();
    try {
      // In cmd line, we specify hds.configuration is hds.xml
      if (System.getProperty("hds.configuration") == null) {
        return config;
      }
      config.addResource(
          new FileInputStream(
              System.getProperty("hds.configuration")));
    } catch (IOException e) {
      LOG.error(e);
    }
    return config;
  }

  public static void closeWithLog(final AutoCloseable closable,
      final Log log) {
    if (closable == null) {
      return;
    }
    try {
      closable.close();
    } catch (Exception e) {
      if (log != null) {
        log.error("Failed to close object", e);
      }
    }
  }

  public static String changeUserFormat(String unformatUser) {
    if (unformatUser.contains("\\") && !unformatUser.contains(";")) {
      return unformatUser.replaceFirst("\\\\", ";");
    }
    return unformatUser;
  }

  public static long getResultSize(Result r) {
    long resultSize = 0;
    for (Cell c : r.rawCells()) {
      KeyValue kv = (KeyValue) c;
      resultSize += kv.heapSize();
    }
    return resultSize;
  }

  public static boolean containsWildCard(String str) {
    if (str.contains("*") || str.contains("?")) {
      return true;
    }
    return false;
  }

  // in hds, we use wildcard(only use '?' & '*') not regex.
  public static boolean isWildCard(String str) {
    boolean escaping = false;
    for (char currentChar : str.toCharArray()) {
      switch (currentChar) {
        case '*':
          if (!escaping) {
            return true;
          }
          escaping = false;
          break;
        case '?':
          if (!escaping) {
            return true;
          }
          escaping = false;
          break;
        case '\\':
          escaping = !escaping;
          break;
        default:
          escaping = false;
      }
    }
    return false;
  }

  // for convenience and generalization, we convert wildcard into regex.
  public static String convertWildCardToRegEx(String str) {
    str = str.trim();
    int strLen = str.length();
    StringBuilder sb = new StringBuilder(strLen);
    boolean escaping = false;
    sb.append("^");
    for (char currentChar : str.toCharArray()) {
      switch (currentChar) {
        case '*':
          if (escaping) {
            sb.append("\\*");
          } else {
            sb.append("(.*)");
          }
          escaping = false;
          break;
        case '?':
          if (escaping) {
            sb.append("\\?");
          } else {
            sb.append("(.)");
          }
          escaping = false;
          break;
        case '.':
        case '(':
        case ')':
        case '+':
        case '|':
        case '^':
        case '$':
        case '@':
        case '%':
        case '[':
        case ']':
        case '{':
        case '}':
        case ',':
          sb.append('\\');
          sb.append(currentChar);
          escaping = false;
          break;
        case '\\':
          if (escaping) {
            sb.append("\\\\");
            escaping = false;
          } else {
            escaping = true;
          }
          break;
        default:
          escaping = false;
          sb.append(currentChar);
      }
    }
    sb.append("$");
    return sb.toString();
  }

  public static String getTmpPath(String originPath) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(originPath).append(HDSConstants.DEFAULT_BACKUP_FILEEXT)
        .append(".").append(UUID.randomUUID().toString());
    return sb.toString();
  }
  
  public static String buildHdsDataUri(String catalog, String key, String version) {
    String realKey;
    if (version.equals(HDSConstants.HDS_VERSION_V2)) {
      realKey = key.substring(key.indexOf(HDSConstants.DEFAULT_ROWKEY_SEPARATOR) +
              HDSConstants.DEFAULT_ROWKEY_SEPARATOR.length());
    } else {
      realKey = key;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("hds:///").append(catalog).append("/").append(realKey);
    return sb.toString();
  }

  public static Map<String, Double> getHBaseDataLocationPercentage(
      final HBaseConnection conn, final TableName tableName, final byte[] key)
      throws IOException {
    Map<String, Double> dataOwner = new TreeMap<>();
    dataOwner.put(conn.getDataLocation(tableName, key), 1.0d);
    return dataOwner;
  }

  public static Map<String, Double> getHdfsDataLocationPercentage(
      FileSystem fs, String filePath) throws IOException {
    Map<String, AtomicInteger> hostCount = new TreeMap<>();
    Path srcPath = new Path(filePath);
    if (!fs.exists(srcPath)) {
      throw new IOException("file not found." + srcPath);
    }
    String fileName = FilenameUtils.getName(filePath);
    FileStatus fileStatus = fs.getFileStatus(srcPath);
    BlockLocation[] blkLocations = fs.getFileBlockLocations(
        fileStatus, 0, fileStatus.getLen());
    int totalBlock = 0;
    for (BlockLocation blkLocation : blkLocations) {
      String[] hosts = blkLocation.getHosts();
      for (String host : hosts) {
        AtomicInteger count = hostCount.get(host);
        if (count == null) {
          count = new AtomicInteger(0);
          hostCount.put(host, count);
        }
        count.incrementAndGet();
        totalBlock++;
      }
    }
    if (hostCount.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Double> dataOwner = new TreeMap<>();
    for (String host : hostCount.keySet()) {
      double occupyPercentage
          = (float) hostCount.get(host).get() / (float) totalBlock;
      dataOwner.put(host, occupyPercentage);
    }
    return dataOwner;
  }

  public static void createNamespaceIfNotExisted(Connection conn, String namespace) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      NamespaceDescriptor nsDesc;
      try {
        admin.getNamespaceDescriptor(namespace);
      } catch (NamespaceNotFoundException ex) {
        nsDesc = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(nsDesc);
      }
    }
  }

  public static String createResponse(List<DataRecord> les) {
    JsonObject response = new JsonObject();
    JsonArray dataInfo = new JsonArray();
    les.stream().map((DataRecord le) -> {
      return HDSUtil.createJson(le);
    }).forEach((JsonObject element) -> {
      dataInfo.add(element);
    });
    response.add(HDSConstants.LIST_DATAINFO, dataInfo);
    return response.toString();
  }

  public static JsonObject createJson(DataRecord le) {
    if (le == null) {
      return null;
    }
    long time = le.getTime();
    DateTimeFormatter fmt = DateTimeFormat
        .forPattern(HDSConstants.DEFAULT_TIME_FORMAT);
    DateTime dt = new DateTime(time,
        DateTimeZone.forID(HDSConstants.DEFAULT_TIME_ZONE));
    JsonObject element = new JsonObject();
    JsonPrimitive uri = le.getUri() == null ? null : new JsonPrimitive(le.getUri()),
        location = le.getLocation() == null ? null : new JsonPrimitive(le.getLocation().name()),
        name = le.getName() == null ? null : new JsonPrimitive(le.getName()),
        size = new JsonPrimitive(le.getSize()),
        dateTime = new JsonPrimitive(fmt.print(dt)),
        fileType = le.getFileType().isPresent() ? new JsonPrimitive(le.getFileType().get().name()) : null;
    element.add(HDSConstants.LIST_URI, uri);
    element.add(HDSConstants.LIST_LOCATION, location);
    element.add(HDSConstants.LIST_NAME, name);
    element.add(HDSConstants.LIST_SIZE, size);
    element.add(HDSConstants.LIST_TIME, dateTime);
    element.add(HDSConstants.LIST_FILETYPE, fileType);
    Map<String, Double> dataOwner = le.getDataOwner();
    JsonArray dataOwners = new JsonArray();

    dataOwner.forEach((String host, Double ratio) -> {
      JsonObject dataOwnerObj = new JsonObject();
      DecimalFormat df = new DecimalFormat("#.0");
      Double r = Double.valueOf(df.format(ratio));
      dataOwnerObj.add(HDSConstants.LIST_DATAOWNER_HOST,
          new JsonPrimitive(host));
      dataOwnerObj.add(HDSConstants.LIST_DATAOWNER_RATIO,
          new JsonPrimitive(ratio));
      dataOwners.add(dataOwnerObj);
    });
    element.add(HDSConstants.LIST_DATAOWNER, dataOwners);
    return element;
  }
}
