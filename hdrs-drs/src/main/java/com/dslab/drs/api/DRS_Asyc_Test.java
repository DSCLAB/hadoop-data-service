package com.dslab.drs.api;

import com.dslab.drs.exception.DrsArgumentsException;
import com.dslab.drs.exception.DrsClientException;

/**
 *
 * @author CDCLAB
 */
public class DRS_Asyc_Test {

  public static void main(String[] args) throws DrsClientException, DrsArgumentsException {

    System.out.println("Start Asyc...");

    //curl -X GET http://192.168.103.94:8000/dataservice/v1/run -G --data-urlencode code=ftp://\$ftp-caca/home/hduser/ftp-code/odfs_r_full_indicator_v9.r --data-urlencode data=ftp://\$ftp-caca/home/hduser/ftp-data/ftp_input.txt --data-urlencode config=ftp://\$ftp-caca/home/hduser/ftp-config/yarn-dispatch.xml --data-urlencode codeout=Output --data-urlencode copyto=ftp://\$ftp-caca/home/hduser/ftp-output/
    String code = "ftp://$ftp-caca/home/hduser/ftp-code/odfs_r_full_indicator_v9.r";
    String data = "ftp://$ftp-caca/home/hduser/ftp-data/ftp_input.txt";
    String config = "ftp://$ftp-caca/home/hduser/ftp-config/yarn-dispatch.xml";
    String codeout = "Output";
    String copyto = "ftp://$ftp-caca/home/hduser/ftp-output/";

    DRSClient client = new DRSClient();
    client.setHdsAccessAddress("http://HOST-ADDRESS:8000/dataservice/v1/access");
    client.setDrsConfigLocation("hdfs:///user/hbase/drs.xml");

    client.init(code, data, config, codeout, copyto,null);
    client.asyncStart();
    ServiceStatus serviceStatus = client.getServiceStatus();

    System.out.println("\n" + serviceStatus.toJSON() + "\n");

  }
}
