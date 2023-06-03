/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.metrics2.lib.HdsMetricsSource.Rate.READ;
import static org.apache.hadoop.metrics2.lib.HdsMetricsSource.Rate.WRITE;
import umc.cdc.hds.core.Protocol;

/**
 *
 * @author brandboat
 */
public class HdsMetricsSourceImpl
    extends HdsBaseSourceImpl implements HdsMetricsSource {

  private HdsMetricsWrapper hdsWrapper;
  private MutableGaugeLong ftpRead;
  private MutableGaugeLong ftpWrite;
  private MutableGaugeLong sftpRead;
  private MutableGaugeLong sftpWrite;
  private MutableGaugeLong smbRead;
  private MutableGaugeLong smbWrite;
  private MutableGaugeLong localRead;
  private MutableGaugeLong localWrite;
  private MutableGaugeLong hdsRead;
  private MutableGaugeLong hdsWrite;
  private MutableGaugeLong hdfsRead;
  private MutableGaugeLong hdfsWrite;
  private MutableGaugeLong fileRead;
  private MutableGaugeLong fileWrite;
  private MutableGaugeLong httpRead;
  private MutableGaugeLong httpWrite;
  private MutableGaugeLong jdbcRead;

  private volatile static HdsMetricsSourceImpl INSTANCE;

  public static HdsMetricsSourceImpl getInstance(HdsMetricsWrapper hdsWrapper) {
    if (INSTANCE == null) {
      synchronized (HdsMetricsSourceImpl.class) {
        if (INSTANCE == null) {
          INSTANCE = new HdsMetricsSourceImpl(hdsWrapper);
        }
      }
    }
    return INSTANCE;
  }

  private HdsMetricsSourceImpl(HdsMetricsWrapper hdsWrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    this.hdsWrapper = hdsWrapper;
    ftpRead = metricsRegistry.newGaugeLong(FTP_READ_RATE, FTP_READ_RATE_DESC, 0);
    ftpWrite = metricsRegistry.newGaugeLong(FTP_WRITE_RATE, FTP_WRITE_RATE_DESC, 0);
    smbRead = metricsRegistry.newGaugeLong(SMB_READ_RATE, SMB_READ_RATE_DESC, 0);
    smbWrite = metricsRegistry.newGaugeLong(SMB_WRITE_RATE, SMB_WRITE_RATE_DESC, 0);
    localRead = metricsRegistry.newGaugeLong(LOCAL_READ_RATE, LOCAL_READ_RATE_DESC, 0);
    localWrite = metricsRegistry.newGaugeLong(LOCAL_WRITE_RATE, LOCAL_WRITE_RATE_DESC, 0);
    hdsRead = metricsRegistry.newGaugeLong(HDS_READ_RATE, HDS_READ_RATE_DESC, 0);
    hdsWrite = metricsRegistry.newGaugeLong(HDS_WRITE_RATE, HDS_WRITE_RATE_DESC, 0);
    hdfsRead = metricsRegistry.newGaugeLong(HDFS_READ_RATE, HDFS_READ_RATE_DESC, 0);
    hdfsWrite = metricsRegistry.newGaugeLong(HDFS_WRITE_RATE, HDFS_WRITE_RATE_DESC, 0);
    fileRead = metricsRegistry.newGaugeLong(FILE_READ_RATE, FILE_READ_RATE_DESC, 0);
    fileWrite = metricsRegistry.newGaugeLong(FILE_WRITE_RATE, FILE_WRITE_RATE_DESC, 0);
    httpRead = metricsRegistry.newGaugeLong(HTTP_READ_RATE, HTTP_READ_RATE_DESC, 0);
    httpWrite = metricsRegistry.newGaugeLong(HTTP_WRITE_RATE, HTTP_WRITE_RATE_DESC, 0);
    jdbcRead = metricsRegistry.newGaugeLong(JDBC_READ_RATE, JDBC_READ_RATE_DESC, 0);
  }

  private HdsMetricsSourceImpl(
      String metricsName,
      String metricsDesc,
      String metricsContext,
      String metricsJmxContext) {
    super(metricsName, metricsDesc, metricsContext, metricsJmxContext);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder mb = collector.addRecord("");
    if (hdsWrapper != null) {
      collectMetrics(mb);
    }
    metricsRegistry.snapshot(mb, all);
  }

  private MetricsRecordBuilder collectMetrics(MetricsRecordBuilder mb) {
    mb
        .addGauge(HdsMetricsHelper.info(SHORT_TERM_TASK_NUM, SHORT_TERM_TASK_NUM_DESC),
            hdsWrapper.getShortTaskNum())
        .addGauge(HdsMetricsHelper.info(LONG_TERM_TASK_NUM, LONG_TERM_TASK_NUM_DESC),
            hdsWrapper.getLongTaskNum())
        .addGauge(HdsMetricsHelper.info(FTP_CONNECTION_NUM, FTP_CONNECTION_NUM_DESC),
            hdsWrapper.getFtpConnNum())
        .addGauge(HdsMetricsHelper.info(JDBC_CONNECTION_NUM, JDBC_CONNECTION_NUM_DESC),
            hdsWrapper.getJdbcConnNum())
        .addGauge(HdsMetricsHelper.info(SHARED_MEM_TOTAL, SHARED_MEM_TOTAL_DESC),
            hdsWrapper.getSharedMemTotal())
        .addGauge(HdsMetricsHelper.info(SHARED_MEM_TAKEN, SHARED_MEM_TAKEN_DESC),
            hdsWrapper.getSharedMemTaken())
        .addGauge(HdsMetricsHelper.info(READ_LOCK_NUM, READ_LOCK_NUM_DESC),
            hdsWrapper.getReadLockNum())
        .addGauge(HdsMetricsHelper.info(WRITE_LOCK_NUM, WRITE_LOCK_NUM_DESC),
            hdsWrapper.getWriteLockNum());
    return mb;
  }

  @Override
  public void updateReadWriteRate(Protocol ds, Rate r, long val) {
    switch (ds) {
      case ftp: {
        if (r.equals(WRITE)) {
          ftpWrite.set(val);
        } else if (r.equals(READ)) {
          ftpRead.set(val);
        }
        break;
      }
      case smb: {
        if (r.equals(WRITE)) {
          smbWrite.set(val);
        } else if (r.equals(READ)) {
          smbRead.set(val);
        }
        break;
      }
      case local: {
        if (r.equals(WRITE)) {
          localWrite.set(val);
        } else if (r.equals(READ)) {
          localRead.set(val);
        }
        break;
      }
      case hds: {
        if (r.equals(WRITE)) {
          hdsWrite.set(val);
        } else if (r.equals(READ)) {
          hdsRead.set(val);
        }
        break;
      }
      case hdfs: {
        if (r.equals(WRITE)) {
          hdfsWrite.set(val);
        } else if (r.equals(READ)) {
          hdfsRead.set(val);
        }
        break;
      }
      case file: {
        if (r.equals(WRITE)) {
          fileWrite.set(val);
        } else if (r.equals(READ)) {
          fileRead.set(val);
        }
        break;
      }
      case http: {
        if (r.equals(WRITE)) {
          httpWrite.set(val);
        } else if (r.equals(READ)) {
          httpRead.set(val);
        }
        break;
      }
      case jdbc: {
        if (r.equals(READ)) {
          jdbcRead.set(val);
        }
        break;
      }
      case sftp: {
        if (r.equals(WRITE)) {
          sftpWrite.set(val);
        } else if (r.equals(READ)) {
          sftpRead.set(val);
        }
        break;
      }
    }
  }

}
