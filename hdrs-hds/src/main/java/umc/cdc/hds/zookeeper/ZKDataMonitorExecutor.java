/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import umc.cdc.hds.zookeeper.DataMonitor.DataMonitorListener;

/**
 *
 * @author brandboat
 */
public abstract class ZKDataMonitorExecutor extends ZKExecutor
    implements DataMonitorListener {

  public ZKDataMonitorExecutor(Configuration conf, ZNodePath zn) throws Exception {
    super(conf, zn);
  }

  @Override
  public Monitor getMonitor() {
    return new DataMonitor(zk, znode, null, this);
  }

  @Override
  public void closing(KeeperException.Code code) {
    synchronized (this) {
      notifyAll();
    }
  }
}
