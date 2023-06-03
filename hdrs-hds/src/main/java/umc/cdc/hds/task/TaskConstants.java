/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.task;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.core.HDSConstants;

/**
 *
 * @author brandboat
 */
public class TaskConstants {

  public static final String TASK_TABLE_NAME = "TASK";
  public static final String TASK_LOG_TABLE_NAME = "TASKLOG";
  public static final TableName TASK_TABLE = TableName.valueOf(
      HDSConstants.DEFAULT_HBASE_NAMESAPCE_STRING, TASK_TABLE_NAME);
  public static final TableName TASK_LOG_TABLE = TableName.valueOf(
      HDSConstants.DEFAULT_HBASE_NAMESAPCE_STRING, TASK_LOG_TABLE_NAME);
  public static final byte[] TASK_ID_QUALIFIER = Bytes.toBytes("i");
  public static final byte[] TASK_STARTTIME_QUALIFIER = Bytes.toBytes("st");
  public static final byte[] TASK_SERVERNAME_QUALIFIER = Bytes.toBytes("sn");
  public static final byte[] TASK_CLIENTNAME_QUALIFIER = Bytes.toBytes("cn");
  public static final byte[] TASK_TO_QUALIFIER = Bytes.toBytes("t");
  public static final byte[] TASK_ELAPSED_QUALIFIER = Bytes.toBytes("e");
  public static final byte[] TASK_STATE_QUALIFIER = Bytes.toBytes("s");
  public static final byte[] TASK_TRANSFERREDSIZE_QUALIFIER = Bytes.toBytes("ts");
  public static final byte[] TASK_REDIRECTFROM_QUALIFIER = Bytes.toBytes("rd");
  public static final byte[] TASK_PROGRESS_QUALIFIER = Bytes.toBytes("p");
  public static final byte[] TASK_EXPECTEDSIZE_QUALIFIER = Bytes.toBytes("es");
  public static final byte[] TASK_FROM_QUALIFIER = Bytes.toBytes("f");
}
