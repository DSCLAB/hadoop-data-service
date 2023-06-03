package com.dslab.drs.api;

import java.io.IOException;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 *
 * @author caca
 */
public class DRS_watch_history {

  public static void main(String[] args) throws IOException, YarnException, InterruptedException, ClassNotFoundException {
    DRSClient client = new DRSClient();
    int limit = 10;
    client.setHdsAccessAddress("http://HOST-ADDRESS:8000/dataservice/v1/access");
    client.setDrsConfigLocation("hdfs:///user/hbase/drs.xml");
    client.watchHistory(10);
  }

}
