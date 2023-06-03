package com.dslab.drs.simulater.connection.jdbc;

/**
 *
 * @author kh87313
 */
public class ApplicationResourceInfos {

  private String AppId;

  private long avgCpu;
  private long maxCpu;

  private long avgRMem;
  private long maxRMem;

  private long avgJvmMem;
  private long maxJvmMem;

  private long avgProcMem;
  private long maxProcMem;

  public String getAppId() {
    return AppId;
  }

  public void setAppId(String AppId) {
    this.AppId = AppId;
  }

  public long getAvgCpu() {
    return avgCpu;
  }

  public void setAvgCpu(long avgCpu) {
    this.avgCpu = avgCpu;
  }

  public long getMaxCpu() {
    return maxCpu;
  }

  public void setMaxCpu(long maxCpu) {
    this.maxCpu = maxCpu;
  }

  public long getAvgRMem() {
    return avgRMem;
  }

  public void setAvgRMem(long avgRMem) {
    this.avgRMem = avgRMem;
  }

  public long getMaxRMem() {
    return maxRMem;
  }

  public void setMaxRMem(long maxRMem) {
    this.maxRMem = maxRMem;
  }

  public long getAvgJvmMem() {
    return avgJvmMem;
  }

  public void setAvgJvmMem(long avgJvmMem) {
    this.avgJvmMem = avgJvmMem;
  }

  public long getMaxJvmMem() {
    return maxJvmMem;
  }

  public void setMaxJvmMem(long maxJvmMem) {
    this.maxJvmMem = maxJvmMem;
  }

  public long getAvgProcMem() {
    return avgProcMem;
  }

  public void setAvgProcMem(long avgProcMem) {
    this.avgProcMem = avgProcMem;
  }

  public long getMaxProcMem() {
    return maxProcMem;
  }

  public void setMaxProcMem(long maxProcMem) {
    this.maxProcMem = maxProcMem;
  }
}
