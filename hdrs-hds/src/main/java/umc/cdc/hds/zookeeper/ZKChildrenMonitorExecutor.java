/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import umc.cdc.hds.zookeeper.ChildrenMonitor.ChildrenMonitorListener;

/**
 *
 * @author brandboat
 */
public abstract class ZKChildrenMonitorExecutor extends ZKExecutor
    implements ChildrenMonitorListener {

  public ZKChildrenMonitorExecutor(Configuration conf, ZNodePath zn) throws Exception {
    super(conf, zn);
  }

  @Override
  protected Monitor getMonitor() {
    return new ChildrenMonitor(zk, znode, null, this);
  }

  @Override
  public void closing(KeeperException.Code code) {
    synchronized (this) {
      notifyAll();
    }
  }
}
