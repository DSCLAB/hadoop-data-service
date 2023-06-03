package com.dslab.drs.monitor;

import com.dslab.drs.drslog.DrsResourceLog;
import com.dslab.drs.drslog.LogServerManager;
import com.dslab.drs.drslog.LogServerManagerImpl;
import com.dslab.drs.drslog.LogStore;
import com.dslab.drs.exception.DrsLoggerException;
import com.dslab.drs.utils.DrsSystemUtils;
import com.dslab.drs.yarn.application.ContainerGlobalVariable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class DrsMonitorThreadImpl implements DrsMonitorThread, Runnable {

  private static final Log LOG = LogFactory.getLog(DrsMonitorThreadImpl.class);
  
  private final LogStore logStore = LogStore.getLogStore();
  private final String pid;
  private final ProcfsBasedProcessTree pTree;
  private final boolean queueLogOnStore;
  
  private boolean running = true;
  
  public DrsMonitorThreadImpl(boolean queueLogOnStore) {
    this.pid = DrsSystemUtils.getPID();
    this.pTree = new ProcfsBasedProcessTree(pid);
    this.queueLogOnStore = queueLogOnStore;
    initProcessTree();
    LOG.debug("PID= " + pid);
  }
  
  public void initProcessTree() {
    LOG.info("appId:" + ContainerGlobalVariable.APPLICATION_NAME);
    pTree.setSmapsEnabled(false);
    pTree.updateProcessTree();
    pTree.getCpuUsagePercent();

    //確保第一次的 CPU 檢查時間不會太短
    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      Logger.getLogger(DrsMonitorThreadImpl.class.getName()).log(Level.SEVERE, null, ex);
    }
    pTree.updateProcessTree();
    pTree.getCpuUsagePercent();
    
  }
  
  @Override
  public void run() {
    try {
      logResource();
    } catch (DrsLoggerException ex) {
      Logger.getLogger(DrsMonitorThreadImpl.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
  
  public void stop() {
    LOG.info("DrsMonitorThread stopped.");
    running = false;
  }
  
  private void logResource() throws DrsLoggerException {
    pTree.updateProcessTree();
//    pTree.printProcessesInfo(true);

    DrsResourceLog resourceLog = new DrsResourceLog();
    resourceLog.setTime(System.currentTimeMillis());
    resourceLog.setApplicationName(ContainerGlobalVariable.APPLICATION_NAME);
    resourceLog.setContainerId(ContainerGlobalVariable.CONTAINER_ID);
    resourceLog.setCpuUsage((int) pTree.getCpuUsagePercent());
//    resourceLog.setProcMemUsage(pTree.getCumulativeRssmem());
    resourceLog.setJvmMemUsage(pTree.getJvmUsedMemBytes());
    resourceLog.setTask(ContainerGlobalVariable.FILE_URL);
    resourceLog.setLogType(ContainerGlobalVariable.RESOURCE_LOG_TYPE);
    resourceLog.setTaskSize(ContainerGlobalVariable.FILESIZE);
    
    if (queueLogOnStore) {
      logStore.addResourceLog(resourceLog);
    } else {
      LogServerManager logServerManager = LogServerManagerImpl.getLogServerManagerImpl(null);
      logServerManager.addResourceLog(resourceLog);
    }
    
  }
}
