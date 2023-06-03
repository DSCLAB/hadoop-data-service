package com.dslab.drs.api;

import java.util.ArrayList;

/**
 *
 * @author chen10
 */
public class NodeStatus implements java.io.Serializable {

  private boolean isClosed;
  private String nodeName;
  private String containerID;
  private String workingTask;
  private long startTime;
  private long endTime;

  private String status;

  private String containerInterval;
  private int memorySize;

  private ArrayList<String> nodeDoneTask = new ArrayList<>();
  private ArrayList<String> nodeFailTask = new ArrayList<>();           //會重複

  public NodeStatus(String nodeName, String containerID) {
    this.isClosed = false;
    this.nodeName = nodeName;
    this.containerID = containerID;
    this.startTime = System.currentTimeMillis();
    this.workingTask = "";
    this.endTime = 0L;
    this.status = "RUNNING";
    this.containerInterval = "";
    this.memorySize = 0;
  }

  public void setContainerInterval(String interval) {
    this.containerInterval = interval;
  }

  public void setMemorySize(int memSize) {
    this.memorySize = memSize;
  }

  public boolean isSameInterval(int mem) {
    return this.memorySize == mem;
  }

  public void addDone(String failTask, long fileSize) {
    nodeDoneTask.add(failTask + " :size=" + fileSize + " byte");
  }

  public void addFail(String failTask, long fileSize) {
    nodeFailTask.add(failTask + " :size=" + fileSize + " byte");
  }

  public void colseContainer() {
    this.endTime = System.currentTimeMillis();
    isClosed = true;
  }

  public void setWorkingTask(String workingTask) {
    this.workingTask = workingTask;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public boolean getIsClosed() {
    return this.isClosed;
  }

  public String getNodeName() {
    return this.nodeName;
  }

  public String getContainerID() {
    return this.containerID;
  }

  public String getWorkingTask() {
    return this.workingTask;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public String getStatus() {
    return this.status;
  }

  public String getContainerInterval() {
    return this.containerInterval;
  }

  public int getMemSize() {
    return this.memorySize;
  }

  public ArrayList<String> getNodeDoneTask() {
    return this.nodeDoneTask;
  }

  public ArrayList<String> getNodeFailTask() {
    return this.nodeFailTask;
  }

}
