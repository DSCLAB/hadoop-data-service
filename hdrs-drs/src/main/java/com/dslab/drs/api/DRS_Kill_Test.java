package com.dslab.drs.api;

import com.dslab.drs.exception.ApplicationIdNotFoundException;
import com.dslab.drs.exception.DrsClientException;
import com.dslab.drs.exception.KillApplicationException;

/**
 *
 * @author chen10
 */
public class DRS_Kill_Test {

  public static void main(String[] args) throws KillApplicationException, ApplicationIdNotFoundException, DrsClientException, InterruptedException {

    String appId = args[0];

    try (DRSClient client = new DRSClient()) {
      client.setHdsAccessAddress("http://HOST-ADDRESS:8000/dataservice/v1/access");
      client.setDrsConfigLocation("hdfs:///user/hbase/drs.xml");
      client.setKillEnviroment();
      client.kill(appId);
//        ServiceStatus serviceStatus = client.watch(appId);
//        System.out.println("\n"+serviceStatus.toJSON());
    }

  }

}
