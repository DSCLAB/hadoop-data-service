/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import umc.cdc.hds.core.Protocol;

/**
 *
 * @author brandboat
 */
public interface HdsMetricsSource extends HdsBaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "HDS";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HDS";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  // String used for exporting to metrics system.
  String FTP_READ_RATE = "ftpReadRate";
  String FTP_READ_RATE_DESC = "ftp read rate";
  String FTP_WRITE_RATE = "ftpWriteRate";
  String FTP_WRITE_RATE_DESC = "ftp write rate";
  String SMB_READ_RATE = "smbReadRate";
  String SMB_READ_RATE_DESC = "smb read rate";
  String SMB_WRITE_RATE = "smbWriteRate";
  String SMB_WRITE_RATE_DESC = "smb write rate";
  String LOCAL_READ_RATE = "localReadRate";
  String LOCAL_READ_RATE_DESC = "local read rate";
  String LOCAL_WRITE_RATE = "localWriteRate";
  String LOCAL_WRITE_RATE_DESC = "local write rate";
  String HDS_READ_RATE = "hdsReadRate";
  String HDS_READ_RATE_DESC = "hds read rate";
  String HDS_WRITE_RATE = "hdsWriteRate";
  String HDS_WRITE_RATE_DESC = "hds write rate";
  String HDFS_READ_RATE = "hdfsReadRate";
  String HDFS_READ_RATE_DESC = "hdfs read rate";
  String HDFS_WRITE_RATE = "hdfsWriteRate";
  String HDFS_WRITE_RATE_DESC = "hdfs write rate";
  String FILE_READ_RATE = "fileReadRate";
  String FILE_READ_RATE_DESC = "file read rate";
  String FILE_WRITE_RATE = "fileWriteRate";
  String FILE_WRITE_RATE_DESC = "file write rate";
  String HTTP_READ_RATE = "httpReadRate";
  String HTTP_READ_RATE_DESC = "http read rate";
  String HTTP_WRITE_RATE = "httpWriteRate";
  String HTTP_WRITE_RATE_DESC = "http write rate";
  String JDBC_READ_RATE = "jdbcReadRate";
  String JDBC_READ_RATE_DESC = "jdbc read rate";

  String SHORT_TERM_TASK_NUM = "shortTermTaskNum";
  String SHORT_TERM_TASK_NUM_DESC = "short term task number";
  String LONG_TERM_TASK_NUM = "longTermTaskNum";
  String LONG_TERM_TASK_NUM_DESC = "long term task number";
  String FTP_CONNECTION_NUM = "ftpConnNum";
  String FTP_CONNECTION_NUM_DESC = "ftp connection number";
  String JDBC_CONNECTION_NUM = "jdbConnNum";
  String JDBC_CONNECTION_NUM_DESC = "jdbc connection number";
  String READ_LOCK_NUM = "readLockNum";
  String READ_LOCK_NUM_DESC = "read lock number";
  String WRITE_LOCK_NUM = "writeLockNum";
  String WRITE_LOCK_NUM_DESC = "write lock number";
  String SHARED_MEM_TOTAL = "sharedMemTotal";
  String SHARED_MEM_TOTAL_DESC = "total size of shared memory in hds datastorage";
  String SHARED_MEM_TAKEN = "sharedMemTaken";
  String SHARED_MEM_TAKEN_DESC = "num of shared memory taken in hds datastorage";

  public enum Rate {
    READ,
    WRITE
  }

  void updateReadWriteRate(Protocol name, Rate r, long val);

}
