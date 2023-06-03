package com.dslab.drs.simulater;

import static com.dslab.drs.utils.DrsConfiguration.HDS_LOGGER_JDBC_DB;
import static com.dslab.drs.utils.DrsConfiguration.HDS_LOGGER_JDBC_DRIVER;
import static com.dslab.drs.utils.DrsConfiguration.HDS_LOGGER_JDBC_URL;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author kh87313
 */
public class Workload {

  public static final String LOG_LEVEL = "log.level";
  public final static String RM_HTTP_ADDRESS = "resource.manager.http.address";
  public final static String HDS_HOST = "hds.host";
  public static final String LOCAL_TEMP_FOLDER = "local.temp.folder";
  public static final String REMOTE_TEMP_FOLDER = "remote.temp.folder";

  public static final String R_SCRIPT_PATH = "r.script.path";
  public final static String IS_DRS_ASYNC = "async";
  public static final String FILE_EXTENSION = "file.extension";

  public static final String RESOURCE_FOLDER = "tmp/resource/";
  public static final String R_SCRIPTE_FOLDER = "tmp/script/";
  public static final String XML_FOLDER = "tmp/xml/";
  public static final String INPUTFILE_FOLDER = "tmp/inputfile/";

  public static final String FILESIZE = "filesize";
  public static final String FILECOUNT = "filecount";
  public static final String CODEOUT = "codeout";
  public static final String COPYTO = "copyto";
  public static final String CONSOLETO = "consoleto";

  public static final String USERCOUNT = "usercount";
  public static final String FILESIZE_DISTRIBUTION = "filesize.distribution";
  public static final String FILENUMBER_MIN = "file.number.min";
  public static final String FILENUMBER_MAX = "file.number.max";
  public static final String FILE_COPY_METHOD = "file.copy.method";
  public static final String FILE_COPY_FROM = "file.copy.from";

  private final Properties properties;

  public Workload(String configFile) throws IOException {
    this.properties = readConfFile(configFile);
  }

  private Properties readConfFile(String configFile) throws IOException {

    Properties _properties = new Properties();
    _properties.load(new FileInputStream(configFile));
    //因為hdfs:/// 有三條斜線，不能使用Path("","") 
    if (_properties.getProperty(REMOTE_TEMP_FOLDER) != null) {
      String remoteTempFolder = _properties.getProperty(REMOTE_TEMP_FOLDER);
      if (!remoteTempFolder.endsWith("/")) {
        _properties.setProperty(REMOTE_TEMP_FOLDER, remoteTempFolder + "/");
      }
    }

    return _properties;
  }

  private Map<String, String> readTextConf(String textPath) throws IOException {
    HashMap<String, String> map = new HashMap<>();

    try (FileReader fr = new FileReader(textPath); BufferedReader br = new BufferedReader(fr)) {
      String line;
      while ((line = br.readLine()) != null) {//write kv to newConfigElementMap
        line = line.trim();
        String[] kvPair = line.split("=");
        if (kvPair.length == 2) {
          map.put(kvPair[0], kvPair[1]);
        }
      }
    }

    //因為hdfs:/// 有三條斜線，不能使用Path("","") 
    if (map.get("remote.temp.folder") != null) {
      String remoteTempFolder = map.get("remote.temp.folder");
      if (!remoteTempFolder.endsWith("/")) {
        map.put("remote.temp.folder", remoteTempFolder + "/");
      }
    }
    return map;
  }

  public int getIntValue(String name) {
    String user = properties.getProperty(name);

    if (user != null) {
      int userCount = Integer.parseInt(user);
      return userCount;
    }
    return 0;
  }

  public String getStringValue(String name) {
    return properties.getProperty(name);
  }

  public void setValue(String key, String value) {
    properties.setProperty(key, value);
  }

  public Set<Entry<Object, Object>> getAllProperties() {
    return properties.entrySet();
  }

  public Configuration getDbConf() {
    Configuration dbConf = new Configuration();
    dbConf.set(HDS_LOGGER_JDBC_DB, getStringValue(HDS_LOGGER_JDBC_DB));
    dbConf.set(HDS_LOGGER_JDBC_URL, getStringValue(HDS_LOGGER_JDBC_URL));
    dbConf.set(HDS_LOGGER_JDBC_DRIVER, getStringValue(HDS_LOGGER_JDBC_DRIVER));
    return dbConf;
  }

}
