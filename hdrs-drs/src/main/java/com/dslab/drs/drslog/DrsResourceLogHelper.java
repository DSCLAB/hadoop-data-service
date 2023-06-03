package com.dslab.drs.drslog;

import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import com.umc.hdrs.dblog.DbLogger;
import com.umc.hdrs.dblog.LogCell;

/**
 * A helper class - Change DrsResourceLog to LogCell, and store into Logger.
 */
public class DrsResourceLogHelper {

  public static final String TIME = "TIME";
  public static final String APPLICATION_NAME = "APPLICATION_NAME";
  public static final String CONTAINER_ID = "CONTAINER_ID";
  public static final String TASK = "TASK";
  public static final String LOG_TYPE = "LOG_TYPE";
  public static final String TASK_SIZE = "TASK_SIZE";
  public static final String CPU_USAGE = "CPU_USAGE";
  public static final String PROC_MEM_USAGE = "PROC_MEM_USAGE";
  public static final String JVM_MEM_USAGE = "JVM_MEM_USAGE";
//select TIME,APPLICATION_NAME,CONTAINER_ID,CPU_USAGE,PROC_MEM_USAGE,JVM_MEM_USAGE from DRS_RESOURCE;

  public static DbLogger.TableSchema getInfo() throws IOException {
    DbLogger.TableSchemaBuilder info = new DbLogger.TableSchemaBuilder();
    info.setTableName(DrsConfiguration.LOG_DB_RESOURCE_TABLE_NAME);
    info.addColumn(TIME, LogCell.SqlType.DATETIME);
    info.addColumn(APPLICATION_NAME, LogCell.SqlType.STRING);
    info.addColumn(CONTAINER_ID, LogCell.SqlType.STRING);
    info.addColumn(TASK, LogCell.SqlType.STRING);
    info.addColumn(LOG_TYPE, LogCell.SqlType.STRING);
    info.addColumn(TASK_SIZE, LogCell.SqlType.BIGINT);
    info.addColumn(CPU_USAGE, LogCell.SqlType.BIGINT);
    info.addColumn(PROC_MEM_USAGE, LogCell.SqlType.BIGINT);
    info.addColumn(JVM_MEM_USAGE, LogCell.SqlType.BIGINT);
    return info.build();
  }

  public static LogCell toLogCell(DrsResourceLog drsResourceLog) {
    LogCell log = new LogCell();
    log.addSQLCell(LogCell.valueOfTime(TIME, drsResourceLog.getTime()));
    log.addSQLCell(LogCell.valueOfString(APPLICATION_NAME, drsResourceLog.getApplicationName()));
    log.addSQLCell(LogCell.valueOfString(CONTAINER_ID, drsResourceLog.getContainerId()));
    log.addSQLCell(LogCell.valueOfString(TASK, drsResourceLog.getTask()));
    log.addSQLCell(LogCell.valueOfString(LOG_TYPE, drsResourceLog.getLogType()));
    log.addSQLCell(LogCell.valueOfLong(TASK_SIZE, drsResourceLog.getTaskSize()));
    log.addSQLCell(LogCell.valueOfLong(CPU_USAGE, drsResourceLog.getCpuUsage()));
    log.addSQLCell(LogCell.valueOfLong(PROC_MEM_USAGE, drsResourceLog.getProcMemUsage()));
    log.addSQLCell(LogCell.valueOfLong(JVM_MEM_USAGE, drsResourceLog.getJvmMemUsage()));
    return log;
  }
}
