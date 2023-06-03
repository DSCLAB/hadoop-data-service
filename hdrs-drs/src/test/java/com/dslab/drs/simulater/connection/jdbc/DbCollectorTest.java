package com.dslab.drs.simulater.connection.jdbc;

import com.dslab.drs.utils.DrsConfiguration;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import com.umc.hdrs.dblog.DbLogger.DbLogConfig;

/**
 *
 * @author kh87313
 */
public class DbCollectorTest {

//  final String confPath = "/tmp/drs_simulator_tool/source/yarn-dispatch.xml";
//  final DbLogConfig dbConf;

  public DbCollectorTest() {
//    DrsConfiguration drsConfiguration = DrsConfiguration.newDrsConfiguration();
//    drsConfiguration.addResource(new Path(confPath));
//    dbConf = new DbLogConfig(drsConfiguration);
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
  public void testPhoenixSql() {
    
  }

//  @Test
//  public void testDbCollector() {
//    try {
//      DbCollector instance = DbCollector.getDbCollector(dbConf);
//
//      System.out.println("isAllTableExist");
//      assertEquals(true, instance.isAllTableExist());
//
//      System.out.println("selectAllPhaseTable");
//      instance.selectAllPhaseTable();
//
//      System.out.println("selectAllResourceTable");
//      instance.selectAllResourceTable();
//
//      System.out.println("selectContainerList");
//      String applicationId = "";
//      System.out.println("applicationId = " + applicationId);
//      Set<String> result = instance.selectContainerList(applicationId);
//      for (String containerId : result) {
//        System.out.println("Selected containerId:" + containerId);
//      }
//
//      System.out.println("computeContainerResourceUsage");
//      String containerId = "";
//      System.out.println("containerId = " + containerId);
//
//      Optional<DbComputedResult> computeResult = instance.computeContainerResourceUsage(containerId);
//      System.out.println("containerId = " + computeResult.get().avgCpuUsage);
//      System.out.println("containerId = " + computeResult.get().avgJvmMem);
//      System.out.println("containerId = " + computeResult.get().avgProcMem);
//      System.out.println("containerId = " + computeResult.get().maxCpuUsage);
//      System.out.println("containerId = " + computeResult.get().maxJvmMem);
//      System.out.println("containerId = " + computeResult.get().maxProcMem);
//
//    } catch (Exception ex) {
//      ex.printStackTrace();
//      fail();
//    }
//  }
}
