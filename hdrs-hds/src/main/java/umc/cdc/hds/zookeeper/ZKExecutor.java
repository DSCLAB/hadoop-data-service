/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 * @author brandboat
 */
public abstract class ZKExecutor implements Runnable {

  protected final ZooKeeper zk;
  private final Monitor dm;
  protected final ZNodePath znode;
  private final static Log LOG = LogFactory.getLog(ZKExecutor.class);

  public ZKExecutor(Configuration conf, ZNodePath zn) throws Exception {
    zk = ZKAccessor.getInstance(conf).get().getZK();
    znode = zn;
    dm = getMonitor();
    if (dm == null) {
      throw new IOException("Monitor is null.");
    }
  }

  protected abstract Monitor getMonitor();

  @Override
  public void run() {
    try {
      synchronized (this) {
        while (!dm.isDead()) {
          wait();
        }
      }
    } catch (InterruptedException e) {
      LOG.error("ZKExecutor interrupted.");
    }
  }

}
