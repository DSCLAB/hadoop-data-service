/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.task.TaskManager.TaskStateEnum;

/**
 *
 * @author brandboat
 */
public class TaskInfoBuilder {

  private static final Log LOG = LogFactory.getLog(TaskInfoBuilder.class);
  private String id;
  private long elapsed;
  private long startTime;
  private TaskManager.TaskStateEnum stateEnum;
  private String from;
  private String to;
  private long dataSize;
  private long transferredSize;
  private String serverName;
  private String clientName;
  private double progress;
  private String redirectFrom;

  public TaskInfoBuilder() {
  }

  public TaskInfoBuilder setId(String id) {
    this.id = id;
    return this;
  }

  public TaskInfoBuilder setStateEnum(TaskManager.TaskStateEnum state) {
    this.stateEnum = state;
    return this;
  }

  public TaskInfoBuilder setFrom(String from) {
    this.from = from;
    return this;
  }

  public TaskInfoBuilder setTo(String to) {
    this.to = to;
    return this;
  }

  public TaskInfoBuilder setDataSize(long dataSize) {
    this.dataSize = dataSize;
    return this;
  }

  public TaskInfoBuilder setDataTransferSize(long transferredSize) {
    this.transferredSize = transferredSize;
    return this;
  }

  public TaskInfoBuilder setClientName(String clientName) {
    this.clientName = clientName;
    return this;
  }

  public TaskInfoBuilder setServerName(String serverName) {
    this.serverName = serverName;
    return this;
  }

  public TaskInfoBuilder setProgress(double progress) {
    this.progress = progress;
    return this;
  }

  public TaskInfoBuilder setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public TaskInfoBuilder setElapsedTime(long elapsedTime) {
    this.elapsed = elapsedTime;
    return this;
  }

  public TaskInfoBuilder setRedirectFrom(String redirectFrom) {
    this.redirectFrom = redirectFrom;
    return this;
  }

  public TaskInfo build(Result r) throws RuntimeException {
    try {
      id = Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_ID_QUALIFIER));
      elapsed = Bytes.toLong(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_ELAPSED_QUALIFIER));
      startTime = Bytes.toLong(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_STARTTIME_QUALIFIER));
      stateEnum = TaskStateEnum.valueOf(Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_STATE_QUALIFIER)));
      from = Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_FROM_QUALIFIER));
      to = Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_TO_QUALIFIER));
      dataSize = Bytes.toLong(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_EXPECTEDSIZE_QUALIFIER));
      transferredSize = Bytes.toLong(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_TRANSFERREDSIZE_QUALIFIER));
      serverName = Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_SERVERNAME_QUALIFIER));
      clientName = Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_CLIENTNAME_QUALIFIER));
      progress = Bytes.toDouble(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_PROGRESS_QUALIFIER));
      redirectFrom = Bytes.toString(r.getValue(
          HDSConstants.DEFAULT_FAMILY,
          TaskConstants.TASK_REDIRECTFROM_QUALIFIER));
      return new TaskInfo(id, stateEnum, from, to, dataSize,
          transferredSize, clientName, serverName, progress,
          startTime, elapsed, redirectFrom);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public TaskInfo build() throws RuntimeException {
    try {
      if (id == null) {
        throw new RuntimeException("TaskInfo id is null.");
      }
      if (stateEnum == null) {
        throw new RuntimeException("TaskInfo state enum is null.");
      }
      if (from == null) {
        throw new RuntimeException("TaskInfo from is null.");
      }
      if (to == null) {
        throw new RuntimeException("TaskInfo to is null.");
      }
      if (clientName == null) {
        throw new RuntimeException("TaskInfo clientName is null.");
      }
      if (serverName == null) {
        throw new RuntimeException("TaskInfo serverName is null.");
      }
      if (redirectFrom == null) {
        throw new RuntimeException("TaskInfo redirectFrom is null.");
      }
    } catch (Exception ex) {
      LOG.error("TaskInfo Builder error.", ex);
      throw ex;
    }
    return new TaskInfo(id, stateEnum, from, to, dataSize,
        transferredSize, clientName, serverName, progress,
        startTime, elapsed, redirectFrom);
  }

  public class TaskInfo {

    public TaskInfo(String id, TaskManager.TaskStateEnum stateEnum, String from,
        String to, long dataSize, long tansferredSize,
        String clientName, String serverName, double progress,
        long startTime, long elapsed, String redirectFrom) {
    }

    public String getId() {
      return id;
    }

    public TaskManager.TaskStateEnum getStateEnum() {
      return stateEnum;
    }

    public String getFrom() {
      return from;
    }

    public String getTo() {
      return to;
    }

    public long getDataSize() {
      return dataSize;
    }

    public long getDataTransferSize() {
      return transferredSize;
    }

    public String getServerName() {
      return serverName;
    }

    public String getClientName() {
      return clientName;
    }

    public double getProgress() {
      return progress;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getElapsedTime() {
      return elapsed;
    }

    public String getRedirectFrom() {
      return redirectFrom;
    }

  }
}
