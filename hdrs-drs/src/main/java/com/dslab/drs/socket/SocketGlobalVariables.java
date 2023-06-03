package com.dslab.drs.socket;

import com.dslab.drs.api.ServiceStatus;
import com.dslab.drs.tasks.FileInfo;
import com.dslab.drs.tasks.Scheduler;
import com.dslab.drs.yarn.application.containermanager.ContainerManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.net.NetUtils;

/**
 *
 * @author Weli
 */
public class SocketGlobalVariables {

  private boolean isRunning = true;
  private boolean isKilled = false;
  private int expectedContainers;
  private final Set<String> releasedContainerID = new HashSet<>();
  private Scheduler schedule_v2;
  private ServiceStatus serviceStatus;
  private Map<String, List<String>> containeridTaskMap = new HashMap<>();
  private final Map<String, Map<String, Long>> containerID_Task_SizeMap = new HashMap<>();
  private final HashMap<String, Long> containerLastResponseTime = new HashMap<>();
  private boolean isListFinish = false;
  private boolean isFirstListEnd = false;
  private final AtomicInteger batchAcc = new AtomicInteger(0);
  private final AtomicInteger lastBatchAcc = new AtomicInteger(0);
  private long firstRequestTime = 0L;
  private final AtomicInteger regCount = new AtomicInteger(0);

  private ContainerManager cm;

  private static SocketGlobalVariables INSTANCE;

  private SocketGlobalVariables() {
    this.serviceStatus = new ServiceStatus(NetUtils.getHostname());
  }

  public static SocketGlobalVariables getAMGlobalVariable() {
    if (INSTANCE == null) {
      synchronized (SocketGlobalVariables.class) {
        if (INSTANCE == null) {
          INSTANCE = new SocketGlobalVariables();
        }
      }
    }
    return INSTANCE;
  }

  public void setContainerMng(ContainerManager cm) {
    this.cm = cm;
  }

  public ContainerManager getContainerMng() {
    return this.cm;
  }

  public void increaseRegCount() {
    regCount.incrementAndGet();
  }

  public int getRegCount() {
    return regCount.get();
  }

  public void setFirstRequestTime(long t) {
    firstRequestTime = t;
  }

  public long getFirstRequestTime() {
    return firstRequestTime;
  }

  public void setBatchAcc(int size) {
    this.batchAcc.set(size);
  }

  public void increaseBatchAcc(int size) {
    this.batchAcc.addAndGet(size);
  }

  public int getBatchAcc() {
    return this.batchAcc.get();
  }

  public void increaseLastBatchAcc(int size) {
    this.lastBatchAcc.addAndGet(size);
  }

  public void setLastBatchAcc(int size) {
    this.lastBatchAcc.set(size);
  }

  public int getLastBatchAcc() {
    return this.lastBatchAcc.get();
  }

  public void firstListFinished() {
    isFirstListEnd = true;
  }

  public boolean isFirstListEnd() {
    return this.isFirstListEnd;
  }

  public void markListThreadFinished() {
    isListFinish = true;
  }

  public boolean isListThreadFinished() {
    return this.isListFinish;
  }

  public boolean isRunning() {
    return this.isRunning;
  }

  public boolean isKilled() {
    return this.isKilled;
  }

  public Scheduler getScheduler() {
    return this.schedule_v2;
  }

  public synchronized void addServiceStatusTask(ArrayList<FileInfo> tasks) {
    this.serviceStatus.addTasks(tasks);
  }

  public synchronized void setServiceTStatusTaskStatus(String url, String status, List<String> outputfiles) {
    this.serviceStatus.setTaskStatus(url, status, outputfiles);
  }

  public synchronized ServiceStatus getServiceStatus() {
    return this.serviceStatus;
  }

  public void addReleasedContainerID(String containerId) {
    releasedContainerID.add(containerId);
  }

  public boolean isReleasedContainerID(String containerId) {
    return releasedContainerID.contains(containerId);
  }

  public Map<String, List<String>> getContaineridTaskMap() {
    return this.containeridTaskMap;
  }

  public void containerRunningTaskMark(String containerId, String taskName, long taskSize) {
    containeridTaskMap.put(containerId, Collections.singletonList(taskName));
    Map<String, Long> map = new HashMap<>();
    map.put(taskName, taskSize);
    containerID_Task_SizeMap.put(containerId, map);
  }

  public Long getFileSize(String containerId, String fileName) {
    return containerID_Task_SizeMap.get(containerId).get(fileName);
  }

  public Long getContainerLastResponseTime(String containerId) {
    return containerLastResponseTime.get(containerId);
  }

  public void logContainerTime(String containerID, long currentTimeMs) {
    containerLastResponseTime.put(containerID, currentTimeMs);
  }

  public void setIsRunning(boolean b) {
    this.isRunning = b;
  }

  public void setIsKilled(boolean b) {
    this.isKilled = b;
  }

  public void decreaseAMExpectedContainer() {
    this.expectedContainers = this.expectedContainers - 1;
  }

  public void increaseAMExpectedContainer() {
    this.expectedContainers++;
  }

  public void setAMExpectedContainer(int expectedContainers) {
    this.expectedContainers = expectedContainers;
  }

  public int getAMExpectedContainer() {
    return this.expectedContainers;
  }

  public void setScheduler(Scheduler scheduler) {
    this.schedule_v2 = scheduler;
  }

  public void setServiceStatus(ServiceStatus serviceStatus) {
    this.serviceStatus = serviceStatus;
  }

  public void setContaineridTaskMapd(HashMap<String, List<String>> containeridTaskMap) {
    this.containeridTaskMap = containeridTaskMap;
  }

}
