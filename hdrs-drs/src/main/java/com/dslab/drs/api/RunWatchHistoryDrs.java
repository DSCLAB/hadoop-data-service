package com.dslab.drs.api;

import com.dslab.drs.exception.ApplicationIdNotFoundException;
import com.dslab.drs.exception.DrsClientException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class RunWatchHistoryDrs {

  private static final Log LOG = LogFactory.getLog(RunWatchHistoryDrs.class);

  private static final List<String> keyList = new ArrayList<>(9);
  private static final Properties props = new Properties();

  public static void main(String[] args) throws IOException, ApplicationIdNotFoundException, DrsClientException, YarnException {
    try (InputStream inputStream = Files.newInputStream(Paths.get(args[0]))) {
      props.load(inputStream);
      addKey();
      Check(props);
    }
    LOG.debug(runWatchHistoryDrs());
  }

  private static void addKey() {
    keyList.add("hds.access.address");
    keyList.add("drs.config.location");
    keyList.add("watch.history.limit");
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

  private static List<ApplicationStatus> runWatchHistoryDrs() throws IOException, ApplicationIdNotFoundException, DrsClientException, YarnException {
    String HDS_ACCESS_ADDRESS = props.getProperty("hds.access.address");
    String HDS_CONFIG_LOCATION = props.getProperty("drs.config.location");
    int limit = Integer.parseInt( props.getProperty("watch.history.limit"));
    
    DRSClient client = new DRSClient();
    client.setHdsAccessAddress(HDS_ACCESS_ADDRESS);
    client.setDrsConfigLocation(HDS_CONFIG_LOCATION);
    List<ApplicationStatus> applicationStatus = client.watchHistory(limit);
    return applicationStatus;
  }

}
