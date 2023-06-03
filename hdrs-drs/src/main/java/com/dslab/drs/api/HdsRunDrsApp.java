/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class HdsRunDrsApp {

  private static final Log LOG = LogFactory.getLog(HdsRunDrsApp.class);

  private static final List<String> keyList = new ArrayList<>(9);
  private static final Properties props = new Properties();

  public static void main(String[] args) throws IOException {
    try (InputStream inputStream = Files.newInputStream(Paths.get(args[0]))) {
      props.load(inputStream);
      addKey();
      Check(props);
    }
    LOG.debug(runDrsApp());
  }

  private static void addKey() {
    keyList.add("hds.access.address");
    keyList.add("drs.config.location");
    keyList.add("isAsync");
    keyList.add("code");
    keyList.add("data");
    keyList.add("config");
    keyList.add("codeOut");
    keyList.add("copyTo");
    keyList.add("consoleTo");
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

  private static ServiceStatus runDrsApp() throws IOException {
    String HDS_ACCESS_ADDRESS = props.getProperty("hds.access.address");
    String HDS_CONFIG_LOCATION = props.getProperty("drs.config.location");
    boolean isAsync = Boolean.valueOf(props.getProperty("isAsync"));
    String code = props.getProperty("code");
    String data = props.getProperty("data");
    String config = props.getProperty("config");
    String codeOut = props.getProperty("codeOut");
    String copyTo = props.getProperty("copyTo");
    String consoleTo = props.getProperty("consoleTo");

    DRSClient client = new DRSClient();
    client.setHdsAccessAddress(HDS_ACCESS_ADDRESS);
    client.setDrsConfigLocation(HDS_CONFIG_LOCATION);
    ServiceStatus serviceStatus;
    try {
      client.init(code, data, config, codeOut, copyTo, consoleTo);
      if (isAsync) {
        client.asyncStart();
      } else {
        client.start();
        // wait for drs app done.
        LOG.info("RUN1");
        while (!client.isAbort()) {
          LOG.info("RUN middle");
          TimeUnit.SECONDS.sleep(1);
        }
        LOG.info("RUN2");
      }
      serviceStatus = client.getServiceStatus();
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    } finally {
      if (isAsync) {
        client.close();
      }
    }
    return serviceStatus;
  }
}
