package com.dslab.drs.tasks;

import com.dslab.drs.api.NodeStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author caca
 */
public interface Scheduler {

  void init();

  void schedule();

  boolean isComplete();

  boolean hasFreeTasks();

  boolean isListThreadRunning();

  boolean noFreeFileAndSomeFileRunning();

  void markSuccess(String fileName, String nodeName, String containerID);

  void markFail(String fileName, String nodeName, String containerID, String errorMessage,boolean isPreempted);

  FileInfo getNewTask(String nodeName, String containerID);

  HashMap<String, NodeStatus> getNodesStatus();

  int getTaskNumber();

  int getTaskAccNumber();

  int getProgress();

  Long getHdsFileInfoCollectTimeMs();

  List<String> getHostList();

  ArrayList<FileInfo> getInputFileInfos();

  void closeListThread();

}
