package com.dslab.drs.restful.api.response.watch;

import com.dslab.drs.restful.api.json.JsonSerialization;
import com.dslab.drs.restful.api.json.JsonUtils;
import com.dslab.drs.simulater.connection.hds.HdsRequester;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author kh87313
 */
public class WatchResult implements JsonSerialization {

  String applicationID;
  String AMnode;
  String code;
  String data;
  String config;
  String copyto;

  String status;
  String errorMessage;
  boolean isComplete;
  long startTime;

  long elapsed;
  int fileCount;
  int progress;
  long hdsFileInfoCollectTimeMs;

  List<NodeStatusResult> nodesStatus;
  List<NodeStatusResult> tasks;

  public String getApplicationID() {
    return applicationID;
  }

  public String getAMnode() {
    return AMnode;
  }

  public String getCode() {
    return code;
  }

  public String getData() {
    return data;
  }

  public String getConfig() {
    return config;
  }

  public String getCopyto() {
    return copyto;
  }

  public String getStatus() {
    return status;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isIsComplete() {
    return isComplete;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getElapsed() {
    return elapsed;
  }

  public int getFileCount() {
    return fileCount;
  }

  public int getProgress() {
    return progress;
  }

  public long getHdsFileInfoCollectTimeMs() {
    return hdsFileInfoCollectTimeMs;
  }

  public List<NodeStatusResult> getNodesStatus() {
    return nodesStatus;
  }

  public List<NodeStatusResult> getTasks() {
    return tasks;
  }

  public static WatchResult getWatchDrsResult(String appId, Configuration drsConfiguration) throws IOException {
    HdsRequester hdsRequestor = new HdsRequester();
    String watchStr = hdsRequestor.watchDrs(appId, drsConfiguration);
    WatchBody watchBody = JsonUtils.fromJson(watchStr, WatchBody.class);
    return watchBody.getApplications();
  }

  public int getRunningContainer() {
    List<NodeStatusResult> nodeStatusResultList = getNodesStatus();
    if (nodeStatusResultList != null) {
      int runningCount = 0;
      for (NodeStatusResult nodeStatus : nodeStatusResultList) {
        if ("RUNNING".equals(nodeStatus.getStatus())) {
          runningCount++;
        }
      }
      return runningCount;
    }
    return 0;
  }

  public int getTotalContainerNumber() {
    List<NodeStatusResult> nodeStatusResultList = getNodesStatus();
    if (nodeStatusResultList != null) {
      return nodeStatusResultList.size();
    }
    return 0;
  }

}
