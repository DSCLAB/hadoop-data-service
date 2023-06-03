package com.dslab.drs.drslog;

/**
 *
 * @author kh87313
 */
public class DrsPhaseLog implements java.io.Serializable {

  private long time;
  private String applicationName;
  private String containerId;
  private String task;
  private String logType;
  private long runTime;

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

  public void setRunTime(long runTime) {
    this.runTime = runTime;
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

  public long getRunTime() {
    return runTime;
  }
}
