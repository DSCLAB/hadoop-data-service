package com.dslab.drs.drslog;

import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import com.umc.hdrs.dblog.DbLogger.TableSchema;
import com.umc.hdrs.dblog.DbLogger.TableSchemaBuilder;
import com.umc.hdrs.dblog.LogCell;

/**
 * A helper class - Change Drs Phase Log to LogCell, and store into Logger.
 */
public class DrsPhaseLogHelper {
  public static final String TIME = "TIME";
  public static final String APPLICATION_NAME = "APPLICATION_NAME";
  public static final String CONTAINER_ID = "CONTAINER_ID";
  public static final String TASK = "TASK";
  public static final String LOG_TYPE = "LOG_TYPE";
  public static final String RUN_TIME = "RUN_TIME";

  public static TableSchema  getInfo() throws IOException {
    TableSchemaBuilder info = new TableSchemaBuilder();
    info.setTableName(DrsConfiguration.LOG_DB_PHASE_TABLE_NAME);
    info.addColumn(TIME, LogCell.SqlType.DATETIME);
    info.addColumn(APPLICATION_NAME, LogCell.SqlType.STRING);
    info.addColumn(CONTAINER_ID, LogCell.SqlType.STRING);
    info.addColumn(TASK, LogCell.SqlType.STRING);
    info.addColumn(LOG_TYPE, LogCell.SqlType.STRING);
    info.addColumn(RUN_TIME, LogCell.SqlType.BIGINT);
    return info.build();
  }

  public static LogCell toLogCell(DrsPhaseLog drsLog) {
    LogCell log = new LogCell();
    log.addSQLCell(LogCell.valueOfTime(TIME, drsLog.getTime()));
    log.addSQLCell(LogCell.valueOfString(APPLICATION_NAME, drsLog.getApplicationName()));
    log.addSQLCell(LogCell.valueOfString(CONTAINER_ID, drsLog.getContainerId()));
    log.addSQLCell(LogCell.valueOfString(TASK, drsLog.getTask()));
    log.addSQLCell(LogCell.valueOfString(LOG_TYPE, drsLog.getLogType()));
    log.addSQLCell(LogCell.valueOfLong(RUN_TIME, drsLog.getRunTime()));
    return log;
  }

  public static DrsPhaseLog getDrsPhaseLog(long time,String appId, String containerId,String task, String type, long runTime) {
    DrsPhaseLog phaseLog = new DrsPhaseLog();
    phaseLog.setTime(time);
    phaseLog.setApplicationName(appId);
    phaseLog.setContainerId(containerId);
    phaseLog.setTask(task);
    phaseLog.setLogType(type);
    phaseLog.setRunTime(runTime);
    return phaseLog;
  }

}
