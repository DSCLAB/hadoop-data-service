package com.dslab.drs.api;

import com.dslab.drs.tasks.FileInfo;
import java.util.Map;
import com.dslab.drs.restful.api.json.JsonSerialization;
import com.dslab.drs.restful.api.json.JsonUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 *
 * @author caca
 */
public class ServiceStatus implements java.io.Serializable, JsonSerialization, Cloneable {

  private String applicationID;
  private String AMnode;
  private String code;
  private String data;
  private String config;
  private String copyto;

  private String status;
  private String errorMessage = "";
  private boolean isComplete;
  private long startTime;

  private long elapsed;
  private int fileCount;
  private int progress;
  private long hdsFileInfoCollectTimeMs;

  private Map<String, NodeStatus> nodesStatus = new LinkedHashMap<>();
  private ArrayList<FileInfo> tasks;

  public ServiceStatus() {
    AMnode = "";
    code = "";
    data = "";
    config = "";
    copyto = "";
  }

  public ServiceStatus(String AMnode) {
    this.tasks = new ArrayList<>();
    this.AMnode = AMnode;
    this.startTime = System.currentTimeMillis();
    this.elapsed = 0L;
    this.fileCount = 0;
    this.progress = 0;
    this.isComplete = false;
  }

  private int countValidFile(ArrayList<FileInfo> tasks) {
    return (int) tasks.stream().map(info -> info.getSize() > 0).count();
  }

  public void updateNodesStatus(Map<String, NodeStatus> nodesStatus, int progress) {
    this.nodesStatus = mapSort(nodesStatus);
    this.progress = progress;
    this.elapsed = System.currentTimeMillis() - this.startTime;
  }

  private Map<String, NodeStatus> mapSort(Map<String, NodeStatus> nodesStatus) {
    Map<String, NodeStatus> map = new LinkedHashMap<>();
    List<Map.Entry<String, NodeStatus>> list = new ArrayList<>();

    nodesStatus.entrySet().stream().forEach((entry) -> list.add(entry));

    Comparator<Map.Entry> sortByValue = (e1, e2) -> {
      return ((String) ((NodeStatus) e1.getValue()).getContainerID())
              .compareTo((String) ((NodeStatus) e2.getValue()).getContainerID());
    };
    Collections.sort(list, sortByValue);

    list.stream().forEach((e) -> {
      map.put((String) e.getKey(), (NodeStatus) e.getValue());
    });
    list.clear();
    return map;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setClientContext(String code, String data, String config, String copyto) {
    this.code = code;
    this.data = data;
    this.config = config;
    this.copyto = copyto;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public void setData(String data) {
    this.data = data;
  }

  public void setConfig(String config) {
    this.config = config;
  }

  public void setCopyto(String copyto) {
    this.copyto = copyto;
  }

  public String toJSON() {
    return JsonUtils.toJson(this);
  }

  public void setHdsFileInfoCollectTimeMs(long hdsFileInfoCollectTimeMs) {
    this.hdsFileInfoCollectTimeMs = hdsFileInfoCollectTimeMs;
  }

  public void setProgress(int progress) {
    this.progress = progress;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setElapsed(Long elapsed) {
    this.elapsed = elapsed;
  }

  public void setApplicationID(String applicationID) {
    this.applicationID = applicationID;
  }

  public void setIsComplete(boolean isComplete) {
    this.isComplete = isComplete;
  }

  public String getCode() {
    return this.code;
  }

  public String getData() {
    return this.data;
  }

  public String getConfig() {
    return this.config;
  }

  public String getCopyto() {
    return this.copyto;
  }

  public String getApplicationID() {
    return this.applicationID;
  }

  public String getAMnode() {
    return this.AMnode;
  }

  public String getStatus() {
    return status;
  }

  public boolean getIsComplete() {
    return this.isComplete;
  }

  public Long getStartTime() {
    return this.startTime;
  }

  public Long getElapsed() {
    return this.elapsed;
  }

  public int getFileCount() {
    return this.fileCount;
  }

  public int getProgress() {
    return this.progress;
  }

  public Long getHdsFileInfoCollectTimeMs() {
    return this.hdsFileInfoCollectTimeMs;
  }

  public Map<String, NodeStatus> getNodesStatus() {
    return this.nodesStatus;
  }

  public ArrayList<FileInfo> getTasks() {
    return this.tasks;
  }

  public void addTasks(ArrayList<FileInfo> tasks) {
    this.tasks.addAll(tasks);
    this.fileCount = countValidFile(this.tasks);
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public void setTaskStatus(String url, String status, List<String> outputfiles) {
//    System.out.println("serviceStatus start");
    for (FileInfo info : tasks) {
      if (info.getUrl().equals(url)) {
        info.setTaskStatus(status);
        for (String outputfile : outputfiles) {
          status = status + "|" + outputfile;
        }
        info.setTaskStatus(status);
        break;
      }
    }
//    System.out.println("serviceStatus end");
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ServiceStatus clone = (ServiceStatus) super.clone();
    clone.tasks = (ArrayList) this.tasks.clone();
    return clone;
  }

}
