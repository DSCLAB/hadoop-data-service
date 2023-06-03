package com.dslab.drs.api;

import java.io.IOException;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 *
 * @author caca
 */
public class DRS_watch_test {

  public static void main(String[] args) throws IOException, YarnException, InterruptedException, ClassNotFoundException {

    String appId = args[0];
    DRSClient client = new DRSClient();
    client.setHdsAccessAddress("http://HOST-ADDRESS:8000/dataservice/v1/access");
    client.setDrsConfigLocation("hdfs:///user/hbase/drs.xml");

    ServiceStatus serviceStatus = client.watch(appId);
    System.out.println("\n" + serviceStatus.toJSON());

  }

}
