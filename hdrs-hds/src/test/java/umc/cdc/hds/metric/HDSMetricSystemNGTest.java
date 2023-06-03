/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.metric;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import umc.cdc.hds.core.HDSConstants.LOADINGCATEGORY;
import umc.cdc.hds.tools.ThreadPoolManager;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author jpopaholic
 */
public class HDSMetricSystemNGTest {

  static Configuration conf;
  static final LOADINGCATEGORY READ = LOADINGCATEGORY.READ;
  static final LOADINGCATEGORY WRITE = LOADINGCATEGORY.WRITE;

  public HDSMetricSystemNGTest() {

    conf = new Configuration();
    conf.setLong("hds.metric.history.ttl", 1l);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

  @BeforeMethod
  public void setUpMethod() throws Exception {
  }

  @AfterMethod
  public void tearDownMethod() throws Exception {
  }

  @Test
  public void testShort() throws InterruptedException, IOException, Exception {
    try (ShareableObject<MetricSystem> system = HDSMetricSystem.getInstance(conf)) {
      ThreadPoolManager.execute(() -> {
        try {
          try (MetricCounter counter1 = system.get().registeExpectableCounter(READ, 10l)) {
            TimeUnit.SECONDS.sleep(1l);
            System.err.println("Thread1 count byte!");
            counter1.addBytes(5l);
            TimeUnit.SECONDS.sleep(4l);
          }
        } catch (InterruptedException ex) {
        } catch (IOException ex) {
          fail("cannot close by IOException!");
        }
      });
      ThreadPoolManager.execute(() -> {
        try {
          try (MetricCounter counter2 = system.get().registerUnExpectableCounter(READ)) {
            TimeUnit.SECONDS.sleep(1l);
            System.err.println("Thread2 count byte!");
            counter2.addBytes(4l);
          }
        } catch (InterruptedException ex) {
        } catch (IOException ex) {
          fail("cannot close by IOException!");
        }
      });
      long exp1 = 9l;
      int exp1r = 1;
      long exp2 = 5l;
      long exp2r = 1;
      TimeUnit.SECONDS.sleep(2l);
      System.err.println("Start test testGetForNow");
      long result1 = system.get().getAllHistoryBytes(READ);
      int result1r = system.get().getAllHistoryRequests(READ);
      long result2 = system.get().getAllFutureBytes(READ);
      int result2r = system.get().getAllFutureRequests(READ);
      assertEquals(result1, exp1);
      assertEquals(result1r, exp1r);
      assertEquals(result2, exp2);
      assertEquals(result2r, exp2r);
      System.err.println("Start to close!");
      ThreadPoolManager.shutdownNowAll();
    }

  }

  @Test(enabled = false)
  public void testLong() throws InterruptedException, IOException, Exception {
    try (ShareableObject<MetricSystem> system = HDSMetricSystem.getInstance(conf)) {
      ThreadPoolManager.execute(() -> {
        try {
          try (MetricCounter counter1 = system.get().registeExpectableCounter(WRITE, 10l)) {
            TimeUnit.SECONDS.sleep(1l);
            System.err.println("Thread1 count byte!");
            counter1.addBytes(5l);
            TimeUnit.SECONDS.sleep(2l);
          }
        } catch (InterruptedException ex) {
        } catch (IOException ex) {
          fail("cannot close by IOException!");
        }
      });
      TimeUnit.SECONDS.sleep(70l);
      long exp = 0l;
      int expr = 0;
      assertEquals(system.get().getAllHistoryBytes(WRITE), exp);
      assertEquals(system.get().getAllFutureRequests(WRITE), expr);
      ThreadPoolManager.shutdownNowAll();
    }
  }

}
