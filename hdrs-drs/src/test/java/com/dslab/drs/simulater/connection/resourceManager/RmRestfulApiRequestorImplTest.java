//package com.dslab.drs.simulater.connection.resourceManager;
//
//import com.dslab.drs.restful.api.parser.appStatus.AppstateResult;
//import com.dslab.drs.restful.api.parser.application.AppResult;
//import com.dslab.drs.restful.api.parser.node.NodeResult;
//import com.dslab.drs.utils.DrsConfiguration;
//import java.util.List;
//import java.util.Optional;
//import java.util.Set;
//import org.apache.hadoop.fs.Path;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import static org.junit.Assert.*;
//
///**
// *
// * @author kh87313
// */
//public class RmRestfulApiRequestorImplTest {
//
//  final String confPath = "/tmp/drs_simulator_tool/source/yarn-dispatch.xml";
//  final DrsConfiguration conf;
//  final String rmHttpAddressPort = "drs-00:8088";
//  final String applicationIdForTestGetApplicationStatus = "application_1486375154037_0001";
//
//  public RmRestfulApiRequestorImplTest() {
//    conf = DrsConfiguration.newDrsConfiguration();
//    conf.addResource(new Path(confPath));
//  }
//
//  @BeforeClass
//  public static void setUpClass() {
//  }
//
//  @AfterClass
//  public static void tearDownClass() {
//  }
//
//  @Before
//  public void setUp() {
//  }
//
//  @After
//  public void tearDown() {
//  }
//
//  /**
//   * Test of getNodeResourceState method, of class RmRestfulApiRequestorImpl.
//   */
//  @Test
//  public void testGetNodeResourceState() {
//    try {
//      System.out.println("Test getNodeResourceState");
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//      NodeResourceState result = instance.getNodeResourceState(rmHttpAddressPort);
//      System.out.println("getNodeResourceState result:" + result.toString());
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//  /**
//   * Test of getRunningAppId method, of class RmRestfulApiRequestorImpl.
//   *
//   * @throws java.io.IOException
//   */
//  @Test
//  public void testGetRunningAppId() {
//    try {
//      System.out.println("Test getRunningAppId");
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//      Set<String> result = instance.getRunningAppId(rmHttpAddressPort);
//      System.out.println("testGetRunningAppId result:");
//      result.stream().forEach((runningAppId) -> {
//        System.out.println(runningAppId);
//      });
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//  /**
//   * Test of getNodesResult method, of class RmRestfulApiRequestorImpl.
//   */
//  @Test
//  public void testGetNodesResult() {
//    try {
//      System.out.println("getNodesResult");
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//      List<NodeResult> result = instance.getNodesResult(rmHttpAddressPort);
//      System.out.println("getNodesResult result:");
//      result.stream().forEach((nodeResult) -> {
//        System.out.println(nodeResult.getId());
//      });
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//  /**
//   * Test of getApplicationStatus method, of class RmRestfulApiRequestorImpl.
//   */
//  @Test
//  public void testGetApplicationStatus() {
//    try {
//      System.out.println("Test getApplicationStatus");
//
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//      AppstateResult result = instance.getApplicationStatus(rmHttpAddressPort, applicationIdForTestGetApplicationStatus);
//      System.out.println("getApplicationStatus result:");
//      System.out.println(result.getState());
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//  /**
//   * Test of getAppResults method, of class RmRestfulApiRequestorImpl.
//   */
//  @Test
//  public void testGetAppResults() {
//    try {
//      System.out.println("Test getAppResults");
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//      List<AppResult> result = instance.getAppResults(rmHttpAddressPort);
//      System.out.println("getAppResults result:");
//      result.stream().forEach((appResult) -> {
//        System.out.println("application:" + appResult.getId());
//      });
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//  /**
//   * Test of getAfterAppId method, of class RmRestfulApiRequestorImpl.
//   */
//  @Test
//  public void testGetAfterAppId() {
//    try {
//      System.out.println("Test getAfterAppId");
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//      Set<String> result = instance.getAfterAppId(rmHttpAddressPort, "application_0_0");
//      System.out.println("getAfterAppId result:");
//      result.stream().forEach((appId) -> {
//        System.out.println("appId:" + appId);
//      });
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//  @Test
//  public void testCompareNewerApplicationId() {
//    try {
//      System.out.println("Test getAppResults");
//      RmRestfulApiRequestorImpl instance = new RmRestfulApiRequestorImpl();
//
//      String appId_A = "application_1326821518301_0005";
//      String appId_B = "application_1326821518301_0006";
//
//      String appId_C = "application_1326821518303_0005";
//      String appId_D = "application_1326821518304_0003";
//
//      String appId_E = "application_1326821518301_0006_2";
//      String appId_F = "application_abc1326821518301_00d06";
//
//      Optional<String> result = instance.compareNewerApplicationId(appId_A, appId_B);
//      assertEquals("application_1326821518301_0006", result.get());
//
//      result = instance.compareNewerApplicationId(appId_A, appId_C);
//      assertEquals("application_1326821518303_0005", result.get());
//
//      result = instance.compareNewerApplicationId(appId_A, appId_D);
//      assertEquals("application_1326821518304_0003", result.get());
//
//      result = instance.compareNewerApplicationId(appId_A, appId_E);
//      assertFalse(result.isPresent());
//
//      result = instance.compareNewerApplicationId(appId_A, appId_F);
//      assertFalse(result.isPresent());
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
//
//}
