/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author s9514171
 */
public class ThreadPoolManager {

  private static final Log LOG = LogFactory.getLog(ThreadPoolManager.class);

  private static ThreadPoolExecutor pool;
  private static ScheduledThreadPoolExecutor scheduledPool;

  public static synchronized void execute(Runnable task) {
    if (pool == null || pool.isTerminating() || pool.isShutdown()) {
      LOG.info("Creating new pool for task");
      pool = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    }
    pool.execute(task);
  }
  public static synchronized void scheduleWithFixedDelay(Runnable task, long delay, TimeUnit unit) {
    if (scheduledPool == null || scheduledPool.isTerminating() || scheduledPool.isShutdown()) {
      LOG.info("Creating new schedule pool for task");
      scheduledPool = new ScheduledThreadPoolExecutor(1);
    }
    scheduledPool.scheduleWithFixedDelay(task, 0, delay, unit);
  }
  public static void shutdownNowAll() {
    LOG.info("shutdown all pools");
    if (pool != null) pool.shutdownNow();
    if (scheduledPool != null) scheduledPool.shutdownNow();
  }
}
