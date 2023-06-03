package com.dslab.drs.drslog;

/**
 *
 * @author kh87313
 */
public class DrsResourceLog implements java.io.Serializable {

  private long time;
  private String applicationName;
  private String containerId;
  private String task;
  private String logType;
  private long taskSize = -1;
  private long cpuUsage = -1;
  private long procMemUsage = -1;
  private long jvmMemUsage = -1;

  public DrsResourceLog() {
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  public void setTask(String task) {
    this.task = task;
  }

  public void setLogType(String logType) {
    this.logType = logType;
  }

  public void setTaskSize(long taskSize) {
    this.taskSize = taskSize;
  }

  public void setCpuUsage(long cpuUsage) {
    this.cpuUsage = cpuUsage;
  }

  public void setProcMemUsage(long procMemUsage) {
    this.procMemUsage = procMemUsage;
  }

  public void setJvmMemUsage(long jvmMemUsage) {
    this.jvmMemUsage = jvmMemUsage;
  }

  public long getTime() {
    return time;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getContainerId() {
    return containerId;
  }

  public String getTask() {
    return task;
  }

  public String getLogType() {
    return logType;
  }

  public long getTaskSize() {
    return taskSize;
  }

  public long getCpuUsage() {
    return cpuUsage;
  }

  public long getProcMemUsage() {
    return procMemUsage;
  }

  public long getJvmMemUsage() {
    return jvmMemUsage;
  }
}
