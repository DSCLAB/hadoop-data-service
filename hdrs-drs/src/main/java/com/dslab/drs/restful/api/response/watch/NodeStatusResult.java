package com.dslab.drs.restful.api.response.watch;

import com.dslab.drs.restful.api.json.JsonSerialization;
import java.util.List;

/**
 *
 * @author kh87313
 */
public class NodeStatusResult implements JsonSerialization {

  String containerID;
  boolean isClosed;
  String nodeName;
  String workingTask;
  long startTime;
  long endTime;
  String status;
  String containerInterval;
  int memorySize;

  List<String> nodeDoneTask;
  List<String> nodeFailTask;

  public String getContainerID() {
    return containerID;
  }

  public boolean isIsClosed() {
    return isClosed;
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getWorkingTask() {
    return workingTask;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public String getStatus() {
    return status;
  }

  public String getContainerInterval() {
    return containerInterval;
  }

  public int getMemSize() {
    return memorySize;
  }

  public List<String> getNodeDoneTask() {
    return nodeDoneTask;
  }

  public List<String> getNodeFailTask() {
    return nodeFailTask;
  }

}
