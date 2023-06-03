package com.dslab.drs.simulater;

import com.dslab.drs.utils.DrsConfiguration;
import java.io.File;
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
public class SimulaterConfManagerTest {

  private final String XML_PATH = "C:\\Users\\kh873\\Desktop\\0115\\yarn-dispatch.xml";
  private final String TXT_PATH = "C:\\Users\\kh873\\Desktop\\0115\\workload.txt";
  private final String XML_OUTPUTPATH = "C:\\Users\\kh873\\Desktop\\0115\\configuration\\threadConf.txt";

  @BeforeClass
  public static void setUpClass() {

  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
//    deleteFile(XML_OUTPUTPATH);
  }

  @After
  public void tearDown() {
  }

//  @Test
//  public void testGetSimulaterConf() throws Exception {
//    SimulaterConfManager confManager = new SimulaterConfManager(XML_PATH, TXT_PATH);
//    DrsConfiguration drsConfiguration = confManager.getSimulaterConf();
//    DrsConfiguration.checkDefaultValue(drsConfiguration);
//    confManager.createNewConfXML(XML_OUTPUTPATH);
//
//    DrsConfiguration resultConf = DrsConfiguration.newDrsConfiguration();
//    resultConf.addResource(XML_PATH);
//
//    assertTrue( resultConf.getBoolean("hds.logger.enable", false));
//    assertEquals(DrsConfiguration.DRS_RM_CONFIG_LOCATION_DEFAULT, resultConf.get(DrsConfiguration.DRS_RM_CONFIG_LOCATION));
//    assertEquals("testValue", resultConf.get("testName"));
//  }

//  /**
//   * Test of generatorThreadConf method, of class SimulaterConfManager.
//   */
//  @Test
//  public void testGeneratorThreadConf() {
//    System.out.println("generatorThreadConf");
//    SimulaterConfManager instance = null;
//    DrsConfiguration expResult = null;
//    DrsConfiguration result = instance.generatorThreadConf();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//
//  /**
//   * Test of getTxtMap method, of class SimulaterConfManager.
//   */
//  @Test
//  public void testGetTxtMap() {
//    System.out.println("getTxtMap");
//    SimulaterConfManager instance = null;
//    Map<String, String> expResult = null;
//    Map<String, String> result = instance.getTxtMap();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
  private void deleteFile(String file) {
    File xmlOutputPath = new File(file);
    if (xmlOutputPath.exists()) {
      xmlOutputPath.delete();
    }
  }
}
