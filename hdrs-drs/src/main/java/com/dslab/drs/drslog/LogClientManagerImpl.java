package com.dslab.drs.drslog;

import com.dslab.drs.monitor.DrsMonitorThreadImpl;
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
public final class LogClientManagerImpl implements LogClientManager {

  private static final Log LOG = LogFactory.getLog(LogClientManagerImpl.class);
  private static LogClientManager INSTANCE;
  private final ScheduledThreadPoolExecutor exe = new ScheduledThreadPoolExecutor(3);

  private final String host;
  private final int port;
  private final LogSocketClient logSocketClient;
  private final DrsMonitorThreadImpl drsMonitorThread;
  private final ProcMemMonitor procMemMonitor;
  private static boolean resizeEnable;

  private LogClientManagerImpl(String host, int port) {
    this.host = host;
    this.port = port;
    logSocketClient = new LogSocketClient();
    drsMonitorThread = new DrsMonitorThreadImpl(true);
    procMemMonitor = new ProcMemMonitor();
  }

  public static LogClientManager getLogClientManagerImpl(DrsConfiguration conf, String host, int port) {
    if (INSTANCE == null) {
      LOG.info("logEnable:" + conf.get(DrsConfiguration.HDS_LOGGER_ENABLE,
              DrsConfiguration.HDS_LOGGER_ENABLE_DEFAULT));
      boolean logEnable = Boolean.parseBoolean(conf.get(DrsConfiguration.HDS_LOGGER_ENABLE, DrsConfiguration.HDS_LOGGER_ENABLE_DEFAULT));
      INSTANCE = logEnable ? new LogClientManagerImpl(host, port)
              : new LogClientManagerEmpty(conf);
      String resized = conf.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE,
              DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE_DEFAULT);
      LOG.info("resizeEnable:" + resized);
      resizeEnable = Boolean.parseBoolean(resized);
    }
    return INSTANCE;
  }

  @Override
  public void setContainerID(String containerID) {
    procMemMonitor.setContainerID(containerID);
  }

  @Override
  public void startMonitor() {
    procMemMonitor.startMonitor();
  }

  @Override
  public void stopMonitor() {
    procMemMonitor.stopMonitor();
  }

  @Override
  public void startResourceMonitor() {
    //DrsMonitorThread會定期紀錄記憶體、CPU資訊
    exe.scheduleWithFixedDelay(drsMonitorThread, 0, 1, TimeUnit.SECONDS);
  }

  @Override
  public void startLogClientSocket() {
    //定期將已收集的log 資訊傳給ApplicationMaster
    exe.scheduleWithFixedDelay(logSocketClient, 0, 5, TimeUnit.SECONDS);
  }

  @Override
  public void startClientResizeMonitor() {
    if (resizeEnable) {
      LOG.info("RUN PROC MONITOR");
      exe.scheduleWithFixedDelay(procMemMonitor, 0, 1, TimeUnit.SECONDS);
    }
  }

  @Override
  public void stopResourceMonitor() {
    exe.remove(drsMonitorThread);
  }

  @Override
  public void stopLogClientSocket() {
    exe.remove(logSocketClient);
  }

  @Override
  public void stopClientResizeMonitor() {
    exe.remove(procMemMonitor);
  }

  @Override
  public void shutDown() {
    exe.shutdown();
    //傳最後一批 LOG
    logSocketClient.run();
  }

  public class LogSocketClient implements Runnable {

    @Override
    public void run() {
      LogPacket logPacket = DRSContainer.logStore.getLogPacket();
      int logPacketSize = logPacket.getToalCellSize();
      if (logPacketSize == 0) {
        return;
      }

      try (Socket logSocket = new Socket(host, port);) {
        //建立連線。(ip為伺服器端的ip，port為伺服器端開啟的port)
        logSocket.setSoTimeout(60000);
        ObjectOutputStream out = new ObjectOutputStream(logSocket.getOutputStream());
        out.writeObject(logPacket);
        out.flush();
      } catch (IOException e) {
        LOG.info("logSocket Error:" + e.getMessage());
      }
    }
  }

}
