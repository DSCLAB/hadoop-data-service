package umc.cdc.hds.dblog;

import com.umc.hdrs.dblog.DbLogger;
import com.umc.hdrs.dblog.DbLogger.DbLogConfig;
import java.io.IOException;
import umc.cdc.hds.core.HDSConstants;
import com.umc.hdrs.dblog.DbLogger.TableSchema;
import com.umc.hdrs.dblog.DbLogger.TableSchemaBuilder;
import com.umc.hdrs.dblog.LogCell;

/**
 * A helper class - Change HdsLog to LogCell, and store into Logger.
 */
public class HdsLogHelper {

  // INSTANCE
  private volatile static DbLogger INSTANCE;

  public static DbLogger getDbLogger(TableSchema schema, DbLogConfig conf) throws IOException {
    if (INSTANCE == null) {
      synchronized (HdsLogHelper.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new DbLogger(schema, conf);
          } catch (Exception ex) {
            throw new IOException(ex);
          }
        }
      }
    }
    return INSTANCE;
  }

  public static TableSchema getInfo() throws IOException {
    TableSchemaBuilder info = new TableSchemaBuilder();
    info.setTableName(HDSConstants.LOG_DB_TABLE_NAME);
    info.addColumn(HdsLog.START_TIME, LogCell.SqlType.DATETIME);
    info.addColumn(HdsLog.HOST, LogCell.SqlType.STRING);
    info.addColumn(HdsLog.API, LogCell.SqlType.STRING);
    info.addColumn(HdsLog.INPUT, LogCell.SqlType.TEXT);
    info.addColumn(HdsLog.OUTPUT, LogCell.SqlType.TEXT);
    info.addColumn(HdsLog.DATA_SIZE, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.REQUEST_INIT, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.GET_SOURCE, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.GET_LOCK, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.ACTION, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.RELEASE_LOCK, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.RELEASE_SOURCE, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.RESPONSE, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.TRANSFERDATA, LogCell.SqlType.BIGINT);
    info.addColumn(HdsLog.COMMITDATA, LogCell.SqlType.BIGINT);
    return info.build();
  }

  public static LogCell toLogCell(HdsLog hdsLog) {
    LogCell log = new LogCell();
    log.addSQLCell(LogCell.valueOfTime(HdsLog.START_TIME, hdsLog.getRequestStartTime()));
    log.addSQLCell(LogCell.valueOfString(HdsLog.HOST, hdsLog.getHost()));
    log.addSQLCell(LogCell.valueOfString(HdsLog.API, hdsLog.getApi()));
    log.addSQLCell(LogCell.valueOfText(HdsLog.INPUT, hdsLog.getInput()));
    log.addSQLCell(LogCell.valueOfText(HdsLog.OUTPUT, hdsLog.getOutput()));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.DATA_SIZE, hdsLog.getDataSize().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.REQUEST_INIT, hdsLog.getRequestInitElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.GET_SOURCE, hdsLog.getGetSourceElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.GET_LOCK, hdsLog.getGetLockElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.ACTION, hdsLog.getActionElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.RELEASE_LOCK, hdsLog.getReleaseLockElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.RELEASE_SOURCE, hdsLog.getReleaseSourceElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.RESPONSE, hdsLog.getResponseElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.TRANSFERDATA, hdsLog.getTransferDataElapsedTime().orElse(null)));
    log.addSQLCell(LogCell.valueOfLong(HdsLog.COMMITDATA, hdsLog.getCommitElapsedTime().orElse(null)));
    return log;
  }
}
