package com.dslab.drs.drslog;

import com.dslab.drs.monitor.ProcMemMonitor;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.yarn.application.DRSContainer;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class LogClientManagerEmpty implements LogClientManager {

  private static final Log LOG = LogFactory.getLog(LogClientManagerEmpty.class);
  private final ProcMemMonitor procMemMonitor = new ProcMemMonitor();
  private final ScheduledThreadPoolExecutor exe = new ScheduledThreadPoolExecutor(1);
  private final boolean resizeEnable;

  public LogClientManagerEmpty(DrsConfiguration conf) {
    String resized = conf.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE_DEFAULT);
    LOG.info("resizeEnable:" + resized);
    this.resizeEnable = Boolean.parseBoolean(resized);
  }

  @Override
  public void setContainerID(String containerID) {
    procMemMonitor.setContainerID(containerID);
  }

  @Override
  public void startResourceMonitor() {
    //Do nothing
  }

  @Override
  public void stopResourceMonitor() {
    //Do nothing
  }

  @Override
  public void startLogClientSocket() {
    //Do nothing
  }

  @Override
  public void stopLogClientSocket() {
    //Do nothing
  }

  @Override
  public void shutDown() {
    exe.shutdown();
  }

  @Override
  public void startClientResizeMonitor() {
    if (resizeEnable) {
      LOG.info("RUN PROC MONITOR");
      exe.scheduleWithFixedDelay(procMemMonitor, 0, 1, TimeUnit.SECONDS);
    }
  }

  @Override
  public void stopClientResizeMonitor() {
    exe.remove(procMemMonitor);
  }

  @Override
  public void startMonitor() {
    procMemMonitor.startMonitor();
  }

  @Override
  public void stopMonitor() {
    procMemMonitor.stopMonitor();
  }

}
