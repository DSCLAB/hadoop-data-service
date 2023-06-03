package com.dslab.drs.yarn.application.containermanager;

import com.dslab.drs.api.async.AMRMClientAsync;
import com.dslab.drs.monitor.ProcMemInfo;
import com.dslab.drs.socket.SocketGlobalVariables;
import com.dslab.drs.tasks.FileInfoStorage;
import com.dslab.drs.tasks.Scheduler;
import com.dslab.drs.utils.DrsConfiguration;
import static com.dslab.drs.utils.DrsConfiguration.DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.Records;

/**
 *
 * @author Weli
 */
public class ContainerManagerImpl extends ContainerManager {

  private static final Log LOG = LogFactory.getLog(ContainerManagerImpl.class);
  private static ContainerManagerImpl INSTANCE;
  private int MULTIPLE;
  //<memSize -> containerInteval>
  private final Map<Integer, ContainerInterval> containerIntervals = new ConcurrentHashMap<>();
  private final Map<String, Resource> containersInfo = new HashMap<>();//save all container resource information 
  private final Map<Integer, Boolean> intervalExpectedPair = new HashMap<>();
  private final Map<String, ContainerId> launchContainerStringID = new HashMap<>();
  private final Map<ContainerId, NodeId> ContainerMappingNodeID = new HashMap<>();  //save launched containerId and NodeId
  private NMClientAsync nmClient;
  private AMRMClientAsync<ContainerRequest> amRMClient;
  private static DrsConfiguration conf;
  private Scheduler schedule;

  private final static long MB = 1024 * 1024;
  private boolean isWasted = false;

  private int askForContainerNumber = 0;

  private ResizeAlgo ra;
  public static long TOTAL_MEM = 0L;
  public static boolean RESIZE_TRIGGER = false;

  private long releaseTime = 0L;
  private long requestTime = 0L;
  private List<String> resizeRelease = new ArrayList<>();
  private final AtomicInteger releases = new AtomicInteger(0);
  private final AtomicInteger requests = new AtomicInteger(0);

  List<Integer> markFinishedInterval = new ArrayList<>(10);
  private boolean hasTaskFlag = false;
  private boolean flagFirstTime = true;

  private ContainerManagerImpl() {
  }

  public static ContainerManagerImpl getInstance() {
    if (INSTANCE == null) {
      synchronized (ContainerManagerImpl.class) {
        if (INSTANCE == null) {
          INSTANCE = new ContainerManagerImpl();
        }
      }
    }
    return INSTANCE;
  }

  public void setNMClient(NMClientAsync nmClient) {
    this.nmClient = nmClient;
  }

  public void setAMRMClient(AMRMClientAsync<ContainerRequest> amRMClient) {
    this.amRMClient = amRMClient;
  }

  public void setConf(DrsConfiguration conf) {
    this.conf = conf;
    MULTIPLE = conf.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE_DEFAULT);
  }

  public DrsConfiguration getConf() {
    return conf;
  }

  public void setScheduler(Scheduler s) {
    this.schedule = s;
  }

  @Override
  public void updateIntervals(int taskSize, ProcMemInfo procMeminfo) {
    int containerSize = this.containersInfo.get(procMeminfo.getContainerID()).getMemory();
    long procMem = procMeminfo.getMem();
    long avgMem = procMeminfo.getAVGMem();
    if (avgMem == 0) {
      return;
    }
    isResizeInterval(avgMem, containerSize);
    LOG.debug(procMeminfo.getContainerID() + " now Memory = " + procMem);
    LOG.debug(procMeminfo.getContainerID() + " average Memory = " + avgMem);
    LOG.debug("updateIntervals#taskSize=" + taskSize);
    if (this.isWasted) {
      LOG.debug("process Memory is wasted");
      if (containerIntervals.get(containerSize / MULTIPLE) != null) {
        containerIntervals.get(containerSize).setLower(taskSize + 1, false);
        containerIntervals.get(containerSize / MULTIPLE).setUpper(taskSize + 1, false);
        containerIntervals.get(containerSize / MULTIPLE)
                .addFileInfos(containerIntervals.get(containerSize).getFiles());
        containerIntervals.get(containerSize).addIntervalLog();
        containerIntervals.get(containerSize / MULTIPLE).addIntervalLog();
        containerIntervals.get(containerSize)
                .rmFileInfo(containerIntervals.get(containerSize / MULTIPLE).getLastChangeFiles());
        LOG.debug("interval size = " + containerSize + ",boundary = " + containerIntervals.get(containerSize).getBoundary());
      }
      this.isWasted = false;
    } else {
      LOG.debug("process Memory used > 50%");
      //do noting
    }
    procMeminfo.outPutMemLog();
  }

  public void outOfMemoryUpdate(String containerID, int taskSize) {
    LOG.debug("process Memory is OOM");
    LOG.debug("outOfMemoryUpdate#taskSize=" + taskSize);
    int containerSize = this.containersInfo.get(containerID).getMemory();
    if (containerIntervals.get(containerSize * MULTIPLE) != null) {
      containerIntervals.get(containerSize).setUpper(taskSize, true);
      containerIntervals.get(containerSize * MULTIPLE).setLower(taskSize, true);
      containerIntervals.get(containerSize * MULTIPLE)
              .addFileInfos(containerIntervals.get(containerSize).getFiles());
      if (containerIntervals.get(containerSize).getRunningFile().containsKey(containerID)) {
        LOG.info("OOM ,add file into bigger interval");
        containerIntervals.get(containerSize * MULTIPLE)
                .addFileInfo(containerIntervals.get(containerSize).getRunningFile().get(containerID));
        FileInfoStorage.getFileInfoStorage()
                .removeRunningTask(containerIntervals.get(containerSize).rmRunningFiles(containerID));
      }
      containerIntervals.get(containerSize).addIntervalLog();
      containerIntervals.get(containerSize * MULTIPLE).addIntervalLog();
      containerIntervals.get(containerSize)
              .rmFileInfo(containerIntervals.get(containerSize * MULTIPLE).getLastChangeFiles());
      LOG.debug("interval size = " + containerSize
              + ",boundary = " + containerIntervals.get(containerSize).getBoundary());
    } else {
      LOG.info("no bigger size interval");
      this.containerIntervals.get(containerSize)
              .addFileInfo(this.containerIntervals.get(containerSize).getRunningFile().get(containerID));
    }
    this.isWasted = false;
  }

  private void isResizeInterval(long maxMemory, int containerSize) {
    this.isWasted = (maxMemory * 2 - containerSize * MB) < 0;
  }

  //cause interval never change til finished , no remove interval method.
  public void putContainerInterval(final ContainerInterval containerInterval) {
    containerIntervals.put(containerInterval.getMemSize(), containerInterval);
  }

  public void putAllContainerInterval(final Map<Integer, ContainerInterval> map) {
    containerIntervals.putAll(map);
  }

  public void clearContainerInterval() {
    containerIntervals.clear();
  }

  public Set<Map.Entry<Integer, ContainerInterval>> getContainerIntervals() {
    return containerIntervals.entrySet();
  }

  @Override
  public ContainerInterval getContainerInterval(int memSize) {
    return containerIntervals.get(memSize);
  }

  public ContainerInterval getTheFileInterval(long fileSize) {
    for (Map.Entry entry : containerIntervals.entrySet()) {
      if (((ContainerInterval) entry.getValue()).isFileBelongInterval(fileSize)) {
        return (ContainerInterval) entry.getValue();
      }
    }
    LOG.error("no interval fit the file");
    return null;
//    return containerIntervals.entrySet().stream().filter(e -> e.getValue().isFileBelongInterval(fileSize)).findFirst().get().getValue();
  }

  public Map<Integer, Boolean> recoveryUnExpectedContainer() {
    intervalExpectedPair.clear();
    containerIntervals.entrySet().stream().forEach((entry)
            -> intervalExpectedPair.put((int) entry.getKey(), ((ContainerInterval) entry.getValue()).isContainerUnexpected()));
    return this.intervalExpectedPair;
  }

  @Override
  public void release(final String cID) {
    LOG.debug("CMG release " + cID);
    schedule.getNodesStatus().get(cID).setStatus("RESIZE_KILLED");
    resizeRelease.add(cID);
    getContainerInterval(containersInfo.get(cID).getMemory()).increaseCompletedContainerNum();
    getContainerInterval(containersInfo.get(cID).getMemory()).rmContainer(cID);
    ContainerId containerid = launchContainerStringID.get(cID);
    NodeId nodeId = ContainerMappingNodeID.get(containerid);
    nmClient.stopContainerAsync(containerid, nodeId);
  }

  public String getResizeRelease(int index) {
    return resizeRelease.get(index);
  }

  public boolean hasResizeRelease(String cid) {
    for (int i = 0; i < resizeRelease.size(); i++) {
      if (resizeRelease.get(i).equals(cid)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void request(String host, boolean relaxLocality, int askcontainerMemory, int vcore) {
    String[] hosts = {host};
    int askcontainerCore = vcore;
    getContainerInterval(askcontainerMemory).increaseAskForContainerNum();
    LOG.debug(askcontainerMemory + " container number = " + getContainerInterval(askcontainerMemory).getContainerNum());
    askForContainerNumber++;
    int priorityNum = askForContainerNumber;
    //test

    Resource containerResource = Records.newRecord(Resource.class);
    containerResource.setMemory(askcontainerMemory);
    containerResource.setVirtualCores(askcontainerCore);
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(priorityNum);
    ContainerRequest ask = new ContainerRequest(containerResource, hosts, null, priority, relaxLocality);

    //throw request to RM
    amRMClient.addContainerRequest(ask);

    LOG.info("Asking for Container ,Memory :" + askcontainerMemory
            + ",Host :" + host
            + ",Core :" + askcontainerCore
            + ",Priority :" + priority);
  }

  public void resizeContainer(int originMemory, int originNum, int askForMemory, int askForNum) {
    for (int i = 0; i < originNum; i++) {
      String releaseCID = containerIntervals.get(originMemory).getContainer();
      release(releaseCID);
    }
    for (int j = 0; j < askForNum; j++) {
      request("", true, askForMemory, conf.getInt(
              DrsConfiguration.CONTAINER_CPU_VCORES,
              DrsConfiguration.CONTAINER_CPU_VCORES_DEFAULT));
    }
  }

  public void resizeContainer(ContainerResizeArgs... c) {
    for (ContainerResizeArgs a : c) {
      if (a.isRelease) {
        for (int i = 0; i < a.number; i++) {
          SocketGlobalVariables.getAMGlobalVariable().decreaseAMExpectedContainer();
          this.releases.incrementAndGet();
          getContainerInterval(a.memory).decreaseExpectedContaienrNum();
          String releaseCID = containerIntervals.get(a.memory).getContainer();
          if (releaseCID.equals("")) {
            return;
          }
          release(releaseCID);
          LOG.info("Kill " + releaseCID + ",mem = " + a.memory);
        }
      } else {
        for (int j = 0; j < a.number; j++) {
          SocketGlobalVariables.getAMGlobalVariable().increaseAMExpectedContainer();
          this.requests.incrementAndGet();
          getContainerInterval(a.memory).increaseExpectedContaienrNum();
          request("", true, a.memory, conf.getInt(
                  DrsConfiguration.CONTAINER_CPU_VCORES,
                  DrsConfiguration.CONTAINER_CPU_VCORES_DEFAULT));
          LOG.info("request container mem = " + a.memory);
        }
      }
    }
  }

  public static Map<Integer, Integer> parseToContainerPair(int intervalNum, int minSize, int multiple, String containerNumSet) {
    Map<Integer, Integer> pair = new HashMap<>();//container pair <memSize,containerNum>
    String[] containerNum = containerNumSet.trim().split(",");
    int memSize = minSize;
    for (int i = 0; i < intervalNum; i++) {
      pair.put(memSize, Integer.valueOf(containerNum[i]));
      memSize = memSize * multiple;
    }
    return pair;
  }

  public static Map<Integer, ContainerInterval> parseToContainerIntervals(int intervalNum, int minSize, int multiple,
          String intervalBoundary, String containerNumSet) throws ContainerIntervalException {
    Map<Integer, ContainerInterval> map = new HashMap<>();
    String[] boundary = intervalBoundary.trim().split(",");
    String[] containerNum = containerNumSet.trim().split(",");
    calculateTotalMem(minSize, multiple, containerNum);
    for (String containerNums : containerNum) {
      if (Integer.valueOf(containerNums) < 1) {
        throw new ContainerIntervalException("some Container_Interval Container's number < 1");
      }
    }
    int memSize = minSize;
    for (int i = 0; i < intervalNum; i++) {
      map.put(memSize, new ContainerInterval(memSize, Integer.valueOf(boundary[i]), Integer.valueOf(boundary[i + 1]),
              Integer.valueOf(containerNum[i])));
      memSize = memSize * multiple;
    }
    return map;
  }

  public static void calculateTotalMem(int minSize, int multiple, String[] containerNumSet) {
    int memSize = minSize;
    for (String containerNums : containerNumSet) {
      TOTAL_MEM += Integer.valueOf(containerNums) * memSize;
      memSize = memSize * multiple;
    }
  }

  public boolean isMaxInterval(String containerID) {
    int maxMemSize = containerIntervals.keySet().stream().max(Comparator.comparing(k -> k)).get();
    int minMemSize = containerIntervals.keySet().stream().min(Comparator.comparing(k -> k)).get();
    return containersInfo.get(containerID).getMemory() == maxMemSize
            || containersInfo.get(containerID).getMemory() == minMemSize;
  }

  public void putContainerResource(String cID, Resource resource) {
    containerIntervals.get(resource.getMemory()).addContainer(cID);
    this.containersInfo.put(cID, resource);
  }

  @Override
  public Resource getContainerResource(String cID) {
    return this.containersInfo.get(cID);
  }

  public void putLaunchContainerStringID(String cID, ContainerId cId) {
    launchContainerStringID.put(cID, cId);
  }

  public ContainerId getContainerID(String cID) {
    return launchContainerStringID.get(cID);
  }

  public void putContainerMappingNodeID(ContainerId cId, NodeId nId) {
    ContainerMappingNodeID.put(cId, nId);
  }

  public NodeId getContainerMappingNodeID(ContainerId cId) {
    return ContainerMappingNodeID.get(cId);
  }

  public int getAskContainerNum() {
    return this.askForContainerNumber;
  }

  public void showIntervalsLogChange() {
    containerIntervals.entrySet().stream().forEach((entry) -> {
      System.out.println("size =" + entry.getKey());
      entry.getValue().showIntervalChange();
      System.out.println();
    });
  }

  public synchronized void startResizeContainerAlgo(String policy) {
    switch (policy) {
      case "DONETASK":
        ra = ResizeAlgo.getInstance(DoneTaskResizeAlgo.class);
        if (RESIZE_TRIGGER == true) {
          runResize();
        }
        break;
      case "CYCLICAL":
        ra = ResizeAlgo.getInstance(CyclicalResizeAlgo.class);
        runResize();
        break;
      default:
        ra = ResizeAlgo.DEFAULT_ALGO;
        int hasTaskTimeCount = conf.getInt(
                DrsConfiguration.DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT,
                DrsConfiguration.DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT_DEFAULT);
        hasTaskTime(hasTaskTimeCount);
        if (someIntervalFinished() || (hasTaskFlag && flagFirstTime)) {
          long count = containerIntervals.entrySet().stream().filter(e -> e.getValue().getAVGTime() > 0).count();
          LOG.debug("hasTaskTimeCount=" + hasTaskTimeCount + ",now count=" + count);
          runResize();
          if (hasTaskFlag) {
            flagFirstTime = false;
          }
          hasTaskFlag = false;
        }
        break;
    }
    RESIZE_TRIGGER = false;
  }

  private void runResize() {
    ra.run();
    if (ra.isExecute()) {
      ra.execute();
    } else {
      LOG.info("not executing resize");
    }
  }

  private boolean someIntervalFinished() {
    boolean b = false;
    for (Map.Entry<Integer, ContainerInterval> e : containerIntervals.entrySet()) {
      if ((e.getValue().getRunningFilesSize() + e.getValue().getFilesSize()) == 0
              && SocketGlobalVariables.getAMGlobalVariable().isListThreadFinished()) {
        if (isNeeded(e.getKey())) {
          markFinishedInterval.add(e.getKey());
          return true;
        }
      }
    }
    return b;
  }

  private void hasTaskTime(int hasTaskTimeCount) {
    long count = containerIntervals.entrySet().stream().filter(e -> e.getValue().getAVGTime() > 0).count();
    if (count >= hasTaskTimeCount) {
      hasTaskFlag = true;
    }
  }

  private boolean isNeeded(int intervalSize) {
    boolean b = true;
    for (int i = 0; i < markFinishedInterval.size(); i++) {
      if (intervalSize == markFinishedInterval.get(i)) {
        return false;
      }
    }
    return b;
  }

  @Override
  public void turnOnResizeTrigger() {
    RESIZE_TRIGGER = true;
  }

  public long getTotalMem() {
    return TOTAL_MEM;
  }

  public String getIntervalsContainerNumLog() {
    StringBuilder sb = new StringBuilder();
    containerIntervals.entrySet().stream().forEach((e) -> {
      sb.append(e.getValue().getContainerNum()).append(", ");
    });
    return sb.toString();
  }

  @Override
  public void setReleaseTime(long t) {
    releaseTime = t;
  }

  @Override
  public void setRequestTime(long t) {
    requestTime = t;
  }

  @Override
  public long getReleaseTime() {
    return releaseTime;
  }

  @Override
  public long getRequestTime() {
    return requestTime;
  }

  @Override
  public int getReleaseNum() {
    return releases.get();
  }

  @Override
  public int getRequestNum() {
    return requests.get();
  }

}
