package com.dslab.drs.hdsclient;

import com.dslab.drs.simulater.DrsArguments;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
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
public class HdsClientTest {

  public HdsClientTest() {
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

  @Test
  public void testCreateDrsRunUrl() throws MalformedURLException {

    String code = "hdfs:///tmp/drs/odfs_r_full_indicator_v9.r";
    String data = "hdfs:///tmp/drs/inputfile.txt";
    String config = "hdfs:///tmp/drs/yarn-dispatch.xml";
    String codeout = "Output";
    String copyto = "hdfs:///tmp/drs/indicator/output/";
    DrsArguments drsConfiguration = new DrsArguments(code, data, config, codeout, copyto);
    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
    conf.set(DrsConfiguration.DRS_HDS_AUTH_TOKEN, DrsConfiguration.DRS_HDS_AUTH_TOKEN_DEFAULT);

    HdsClient hdClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS));

    URL drsUrl = hdClient.createDrsRunUrl(drsConfiguration, conf, false);
    System.out.println(drsUrl.toString());
    assertEquals("http://HOST-ADDRESS:8000/dataservice/v1/run?&token=sweatshopsdslab&code=hdfs%3A%2F%2F%2Ftmp%2Fdrs%2Fodfs_r_full_indicator_v9.r&data=hdfs%3A%2F%2F%2Ftmp%2Fdrs%2Finputfile.txt&config=hdfs%3A%2F%2F%2Ftmp%2Fdrs%2Fyarn-dispatch.xml&codeout=Output&copyto=hdfs%3A%2F%2F%2Ftmp%2Fdrs%2Findicator%2Foutput%2F", drsUrl.toString());
  }

  @Test
  public void testCreateUpdloadUrl() throws MalformedURLException {
    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
    conf.set(DrsConfiguration.DRS_HDS_AUTH_TOKEN, "");
    HdsClient hdClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS));
    URL uploadUrl = hdClient.createUpdloadUrl("local", "hdfs:///tmp/abc.txt", conf);
    System.out.println("uploadUrl:" + uploadUrl);
    assertEquals("http://HOST-ADDRESS:8000/dataservice/v1/access?from=local%3A%2F%2F%2Flocal&to=hdfs%3A%2F%2F%2Ftmp%2Fabc.txt", uploadUrl.toString());

    conf.set(DrsConfiguration.DRS_HDS_AUTH_TOKEN, DrsConfiguration.DRS_HDS_AUTH_TOKEN_DEFAULT);
    uploadUrl = hdClient.createUpdloadUrl("local", "hdfs:///tmp/abc.txt", conf);

    System.out.println("uploadUrl:" + uploadUrl);
    assertEquals("http://HOST-ADDRESS:8000/dataservice/v1/access?from=local%3A%2F%2F%2Flocal&to=hdfs%3A%2F%2F%2Ftmp%2Fabc.txt&token=sweatshopsdslab", uploadUrl.toString());
  }

}
