/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.monitor;

import com.dslab.drs.utils.DrsSystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class ProcMemMonitor implements DrsMonitorThread, Runnable {

  private static final Log LOG = LogFactory.getLog(ProcMemMonitor.class);

  private final String pid;
  private final ProcfsBasedProcessTree pTree;
  private final ProcMemInfo procMemInfo = ProcMemInfo.getInstance();
  private boolean isStart = false;

  public ProcMemMonitor() {
    this.pid = DrsSystemUtils.getPID();
    this.pTree = new ProcfsBasedProcessTree(pid);
    pTree.updateProcessTree();
  }

  @Override
  public void run() {
    if (isStart) {
      pTree.updateProcessTree();
//      long memory = pTree.getCumulativeRssmem();
//      procMemInfo.updateInfo(memory);
    }
  }

  public void setContainerID(String containerID) {
    procMemInfo.setContainerID(containerID);
  }

  public void startMonitor() {
    procMemInfo.reset();
    LOG.debug("START Monitor");
    this.isStart = true;
  }

  public void stopMonitor() {
    LOG.debug("STOP Monitor");
    this.isStart = false;
  }

}
