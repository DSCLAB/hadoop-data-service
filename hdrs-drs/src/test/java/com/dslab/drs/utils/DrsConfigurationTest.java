package com.dslab.drs.utils;

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
public class DrsConfigurationTest {
  
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
  public void testReplaceHdsConfHost() {
    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
    String before_access_address = DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT;
    String before_list_address = DrsConfiguration.DISPATCH_HDS_LIST_ADDRESS_DEFAULT;
    DrsConfiguration.replaceHdsConfHost(conf, "192.168.103.94");
    String after_access_address = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS,DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT);
    String after_list_address = conf.get(DrsConfiguration.DISPATCH_HDS_LIST_ADDRESS,DrsConfiguration.DISPATCH_HDS_LIST_ADDRESS_DEFAULT);
    assertEquals("http://192.168.103.94:8000/dataservice/v1/access",after_access_address);
    assertEquals("http://192.168.103.94:8000/dataservice/v1/list?from=",after_list_address);
  }
  
}
