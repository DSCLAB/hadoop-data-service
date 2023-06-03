package com.dslab.drs.simulater.connection.hds;

import com.dslab.drs.simulater.DrsArguments;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.net.MalformedURLException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kh87313
 */
public class HdsRequesterTest {

  public HdsRequesterTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

//  @Test
//  public void testRequestDrs() throws MalformedURLException, IOException {
//    HdsRequester instance = new HdsRequester();
//
//    String code = "hdfs:///tmp/drs/odfs_r_full_indicator_v9.r";
//    String data = "hdfs:///tmp/drs/inputfile.txt";
//    String config = "hdfs:///tmp/drs/yarn-dispatch.xml";
//    String codeout = "Output";
//    String copyto = "hdfs:///tmp/drs/indicator/output/";
//    boolean async = true;
//    DrsArguments drsConfiguration = new DrsArguments(code, data, config, codeout, copyto);
//    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
//    DrsConfiguration.checkDefaultValue(conf);
//    DrsConfiguration.replaceHdsConfHost(conf, "192.168.103.94");
//    String response = instance.requestDrs(drsConfiguration,conf,async);
//    System.out.println(response);
//  }
//  @Test
//  public void testWatchAndKillDrs() throws MalformedURLException, IOException {
//    HdsRequester instance = new HdsRequester();
//
//    String code = "hdfs:///tmp/drs/odfs_r_full_indicator_v9.r";
//    String data = "hdfs:///tmp/drs/inputfile.txt";
//    String config = "hdfs:///tmp/drs/yarn-dispatch.xml";
//    String codeout = "Output";
//    String copyto = "hdfs:///tmp/drs/indicator/output/";
//
//    DrsArguments drsConfiguration = new DrsArguments(code, data, config, codeout, copyto);
//    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
//    DrsConfiguration.checkDefaultValue(conf);
//    DrsConfiguration.replaceHdsConfHost(conf, "192.168.103.94");
////    String response = instance.requestDrs(drsConfiguration, conf);
//    System.out.println("response.");
//    System.out.println("Sleep 1 second...");
//    try {
//      Thread.sleep(1000);
//    } catch (Exception e) {
//    }
//    String appId = "application_1484769238643_0080";
//
//    String watchResponse = instance.watchDrs(appId, conf);
//    System.out.println(watchResponse);
//    System.out.println("Sleep 1 second...");
//    try {
//      Thread.sleep(1000);
//    } catch (Exception e) {
//    }
//
//    String killResponse = instance.killDrs(appId, conf);
//    System.out.println(killResponse);
//    System.out.println("Sleep 1 second...");
//    try {
//      Thread.sleep(1000);
//    } catch (Exception e) {
//    }
//    
//    String watchResponse2 = instance.watchDrs(appId, conf);
//    System.out.println(watchResponse2);
//  }
//  @Test
//  public void testUploadToHds() throws Exception {
//    String local = "F:\\test.property";
//    String to = "hdfs:///tmp/test.property";
//    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
//    DrsConfiguration.checkDefaultValue(conf);
//    DrsConfiguration.replaceHdsConfHost(conf, "192.168.103.94");
//    
//    HdsRequester instance = new HdsRequester();
//    instance.uploadToHds(local, to, conf);
//  }
//  @Test
//  public void testDeleteds() throws Exception {
//    String local = "hdfs:///tmp/test.property";
//    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
//    conf.set(DRSConstants.DISPATCH_HDS_ACCESS_ADDRESS, "http://HOST-ADDRESS:8000/dataservice/v1/access");
//    conf.set(DRSConstants.DRS_HDS_AUTH_TOKEN, "sweatshopsdslab");
//    DrsConfiguration.replaceHdsConfHost(conf, "192.168.103.94");
//
//    HdsRequester instance = new HdsRequester();
//    instance.delete(local, conf);
//  }

}
