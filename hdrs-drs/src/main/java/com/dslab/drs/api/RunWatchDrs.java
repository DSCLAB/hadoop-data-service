package com.dslab.drs.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class RunWatchDrs {

  private static final Log LOG = LogFactory.getLog(RunWatchDrs.class);

  private static final List<String> keyList = new ArrayList<>(9);
  private static final Properties props = new Properties();

  public static void main(String[] args) throws IOException {
    try (InputStream inputStream = Files.newInputStream(Paths.get(args[0]))) {
      props.load(inputStream);
      addKey();
      Check(props);
    }
    LOG.debug(runWatchDrs());
  }

  private static void addKey() {
    keyList.add("hds.access.address");
    keyList.add("drs.config.location");

    keyList.add("applicationId");
  }

  private static void Check(Properties p) {
    keyList.stream().filter((key) -> (!p.containsKey(key))).forEach((key) -> {
      try {
        throw new IOException("property not setting :" + key);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    });
  }

  private static ServiceStatus runWatchDrs() throws IOException {
    String HDS_ACCESS_ADDRESS = props.getProperty("hds.access.address");
    String HDS_CONFIG_LOCATION = props.getProperty("drs.config.location");
    String APPID = props.getProperty("applicationId");

    DRSClient client = new DRSClient();
    client.setHdsAccessAddress(HDS_ACCESS_ADDRESS);
    client.setDrsConfigLocation(HDS_CONFIG_LOCATION);
    ServiceStatus serviceStatus = client.watch(APPID);
    return serviceStatus;
  }

}
