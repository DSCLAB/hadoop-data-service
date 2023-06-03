package com.dslab.drs.simulater.connection.jdbc;

import java.sql.Timestamp;

/**
 *
 * @author kh87313
 */
public class DbContainerResult {

  final Timestamp lastQueryTime;
  final String containerId;
  final long avgCpuUsage;
  final long avgJvmMem;
  final long avgProcMem;
  final long maxCpuUsage;
  final long maxJvmMem;
  final long maxProcMem;

  final long maxRMem;

  public DbContainerResult(String containerId, Timestamp lastQueryTime, long avgCpuUsage, long avgJvmMem, long avgProcMem,
          long maxCpuUsage, long maxJvmMem, long maxProcMem, long maxRMem) {
    this.lastQueryTime = lastQueryTime;
    this.containerId = containerId;
    this.avgCpuUsage = avgCpuUsage;
    this.avgJvmMem = avgJvmMem;
    this.avgProcMem = avgProcMem;
    this.maxCpuUsage = maxCpuUsage;
    this.maxJvmMem = maxJvmMem;
    this.maxProcMem = maxProcMem;
    this.maxRMem = maxRMem;
  }

  public Timestamp getLastQueryTime() {
    return lastQueryTime;
  }

  public String getContainerId() {
    return containerId;
  }

  public long getAvgCpuUsage() {
    return avgCpuUsage;
  }

  public long getAvgJvmMem() {
    return avgJvmMem;
  }

  public long getAvgProcMem() {
    return avgProcMem;
  }

  public long getMaxCpuUsage() {
    return maxCpuUsage;
  }

  public long getMaxJvmMem() {
    return maxJvmMem;
  }

  public long getMaxProcMem() {
    return maxProcMem;
  }

  public long getMaxRMem() {
    return maxRMem;
  }

}
