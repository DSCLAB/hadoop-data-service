package com.dslab.drs.drslog;

import com.dslab.drs.exception.DrsLoggerException;
import com.dslab.drs.monitor.DrsMonitorThreadImpl;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.umc.hdrs.dblog.DbLogger;
import com.umc.hdrs.dblog.DbLogger.DbLogConfig;
import com.umc.hdrs.dblog.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public final class LogServerManagerImpl implements LogServerManager {

  private static final Log LOG = LogFactory.getLog(LogServerManagerImpl.class);
  private static LogServerManager INSTANCE;
  private static Logger drsPhaseLog;
  private static Logger drsResourceLog;
  private static DrsConfiguration CONF;
  private final ScheduledThreadPoolExecutor exe = new ScheduledThreadPoolExecutor(2);
  private final DrsMonitorThreadImpl drsMonitorThread;

  private int phaseLogCount = 0;
  private int resourceLogCount = 0;

  private LogServerManagerImpl() {
    drsMonitorThread = new DrsMonitorThreadImpl(false);
  }

  @Override
  public void createDblogTable() throws DrsLoggerException {
    try {
      LOG.info("Create Dblog Table:" + DrsResourceLogHelper.getInfo().getTableName() + "、" + DrsPhaseLogHelper.getInfo().getTableName() + ".");
      drsResourceLog = new DbLogger(DrsResourceLogHelper.getInfo(), new DbLogConfig(CONF));
      drsPhaseLog = new DbLogger(DrsPhaseLogHelper.getInfo(), new DbLogConfig(CONF));
    } catch (SQLException | ClassNotFoundException | IOException ex) {
      throw new DrsLoggerException("Create drs log error:" + ex.toString());
    }
  }

  //注意第一次呼叫決定是 LogServerManagerImpl 或 LogServerEmpty
  public static LogServerManager getLogServerManagerImpl(DrsConfiguration conf) {
    if (INSTANCE == null) {
      LOG.info("logEnable:" + conf.get(DrsConfiguration.HDS_LOGGER_ENABLE,
              DrsConfiguration.HDS_LOGGER_ENABLE_DEFAULT));
      boolean logEnable = Boolean.parseBoolean(conf.get(DrsConfiguration.HDS_LOGGER_ENABLE, DrsConfiguration.HDS_LOGGER_ENABLE_DEFAULT));
      INSTANCE = logEnable ? new LogServerManagerImpl() : new LogServerManagerEmpty();
      CONF = conf;
    }
    return INSTANCE;
  }

  @Override
  public void startResourceMonitor() {
    //DrsMonitorThread會定期紀錄記憶體、CPU資訊
    exe.scheduleWithFixedDelay(drsMonitorThread, 0, 1, TimeUnit.SECONDS);
  }

  @Override
  public void stopResourceMonitor() {
    exe.remove(drsMonitorThread);
  }

  @Override
  public void addPhaseLog(DrsPhaseLog cell) {
//    LOG.debug("addPhaseLog:" + cell.getApplicationName()
//            + "," + cell.getContainerId()
//            + "," + cell.getTask()
//            + "," + cell.getLogType());

    phaseLogCount++;
    try {
      drsPhaseLog.addLog(DrsPhaseLogHelper.toLogCell(cell));
    } catch (Error ex) {
      LOG.info("addPhaseLog:" + ex.getMessage());
    }
  }

  @Override
  public void addResourceLog(DrsResourceLog cell) {
//    LOG.debug("addResourceLog:" + cell.getApplicationName()
//            + "," + cell.getContainerId()
//            + "," + cell.getTask()
//            + "," + cell.getJvmMemUsage());

    resourceLogCount++;
    try {
      drsResourceLog.addLog(DrsResourceLogHelper.toLogCell(cell));
    } catch (Error ex) {
      LOG.info("addPhaseLog:" + ex.getMessage());
    }
  }

  @Override
  public void addLogPacket(LogPacket packet) {
    List<DrsPhaseLog> phaseList = packet.getPhaseList();
    List<DrsResourceLog> resourceList = packet.getResourceList();

    phaseLogCount += phaseList.size();
    resourceLogCount += resourceList.size();

    for (DrsPhaseLog phaseLog : phaseList) {
      addPhaseLog(phaseLog);
    }
    for (DrsResourceLog resourceLog : resourceList) {
      addResourceLog(resourceLog);
    }
  }

  @Override
  public void shutDown() {
    try {
      drsPhaseLog.close();
    } catch (Exception e) {
    }
    try {
      drsResourceLog.close();
    } catch (Exception e) {
    }
    exe.shutdownNow();
  }

  @Override
  public int getPhaseLogCount() {
    return phaseLogCount;
  }

  @Override
  public int getResourceLogCount() {
    return resourceLogCount;
  }

}
