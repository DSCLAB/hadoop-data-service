package com.dslab.drs.simulater;

import com.dslab.drs.restful.api.response.watch.NodeStatusResult;
import com.dslab.drs.restful.api.response.watch.WatchResult;
import com.dslab.drs.simulater.connection.jdbc.ApplicationResourceInfos;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class ApplicationInfos {

  private static final Log LOG = LogFactory.getLog(ApplicationInfos.class);

//  private final Map<String, Set<String>> applicationContainerListMap = new HashMap<>();
//  private final Map<String, Map<String, DbContainerResult>> appIdContainerComputedResultListMap = new HashMap<>();
  private final Map<String, ApplicationResourceInfos> applicationResourceInfosMap = new HashMap<>();
  private final Map<String, Timestamp> containerLastQueryTimeMap = new HashMap<>();
  private final Map<String, Timestamp> watchContainerStartTimeMap = new HashMap<>();
  private final Map<String, Long> watchContainerElapsedTimeMap = new HashMap<>();
  private final Map<String, WatchResult> watchResultMap = new HashMap<>();
  //  final Map<String, Set<DbContainerResult>> appIdComputedResultMap;

  Set<String> afterAppIds = new HashSet<>();
  Set<String> runningAppIds = new HashSet<>();

  private final Map<String, Long> requestMemoryMap = new HashMap<>();
  private final Map<String, Long> requestVcoresMap = new HashMap<>();
//  Optional<Long> requestMemory;
//  Optional<Long> requestVcores;

  public void addApplicationResourceInfos(String appId, Optional<ApplicationResourceInfos> applicationResourceInfos) {
    if (applicationResourceInfos.isPresent()) {
      applicationResourceInfosMap.put(appId, applicationResourceInfos.get());
    }
  }

  public Optional<ApplicationResourceInfos> getApplicationResourceInfos(String appId) {
    ApplicationResourceInfos applicationResourceInfos = applicationResourceInfosMap.get(appId);
    if (applicationResourceInfos != null) {
      return Optional.of(applicationResourceInfos);
    }
    return Optional.empty();
  }

  public double getThroughput() {
    int totalWatchProgress = getTotalWatchProgress();
    Optional<Long> elapsedTime = getElapsedTimeMs();
    if (elapsedTime.isPresent()) {
      if (elapsedTime.get() > 0) {
        return ((double) (totalWatchProgress * 1000)) / ((double) elapsedTime.get());
      }

    }
    return -1;
  }

  //第一個Application啟動的時間  和最後一個寫入DB log的時間差
  //t/6je/ 
  public Optional<Long> getElapsedTimeMs() {
    Optional<Timestamp> earilyestStartTimeStamp = getWatchEarliestStartTimestamp();
    Optional<Timestamp> latestQueryTimeStamp = getLatestQueryTimestamp();

    if (earilyestStartTimeStamp.isPresent() && latestQueryTimeStamp.isPresent()) {
      Long elapsed = latestQueryTimeStamp.get().getTime() - earilyestStartTimeStamp.get().getTime();
      return Optional.of(elapsed);
    }
    return Optional.empty();
  }

  public long getTotalWatchElapsedTimeMs() {
    long totalElapsed = 0;
    for (WatchResult watchResult : watchResultMap.values()) {
      totalElapsed += watchResult.getElapsed();
    }
    return totalElapsed;
  }

  public Optional<Timestamp> getLastQueryTime(String container) {
    if (containerLastQueryTimeMap.containsKey(container)) {
      return Optional.of(containerLastQueryTimeMap.get(container));
    } else {
      return Optional.empty();
    }
  }

  public int getTotalWatchProgress() {
    int totalProgress = 0;
    for (String appId : afterAppIds) {
      Optional<WatchResult> watchResult = getWatchResult(appId);
      if (watchResult.isPresent()) {
        totalProgress += watchResult.get().getProgress();
      }
    }
    return totalProgress;
  }

  public int getTotalWatchFileCount() {
    int totalFileCount = 0;
    for (String appId : afterAppIds) {
      Optional<WatchResult> watchResult = getWatchResult(appId);
      if (watchResult.isPresent()) {
        totalFileCount += watchResult.get().getFileCount();
      }
    }
    return totalFileCount;
  }

  public String getFileSuccessRateString() {
    double fileSuccessRate = 0;
    int totalFileCount = getTotalWatchFileCount();
    int progress = getTotalWatchProgress();
    if (totalFileCount != 0) {
      fileSuccessRate = (double) progress * 100 / (double) totalFileCount;
      DecimalFormat formatter = new DecimalFormat("###.##");
      return formatter.format(fileSuccessRate);
    }

    return "0.00";
  }

  public void setAfterAppIds(Set<String> afterAppIds) {
    if (afterAppIds != null) {
      this.afterAppIds = afterAppIds;
    }
  }

  public Set<String> getAfterAppIds() {
    return this.afterAppIds;
  }

  public void setRunningAppIds(Set<String> runningAppIds) {
    if (runningAppIds != null) {
      this.runningAppIds = runningAppIds;
    }
  }

  public Set<String> getRunningAppIds() {
    return this.runningAppIds;
  }

  public void setWatchResult(String applicationId, WatchResult watchResult) {
    if (watchResult != null) {
      this.watchResultMap.put(applicationId, watchResult);
      setWatchContainerStartTimeAndElaspsedTime(watchResult);
    }
  }

  public Optional<WatchResult> getWatchResult(String applicationId) {
    WatchResult watchResult = this.watchResultMap.get(applicationId);
    if (watchResult != null) {
      return Optional.of(watchResult);
    } else {
      return Optional.empty();
    }
  }

  public Optional<Timestamp> getWatchEarliestStartTimestamp() {
    Timestamp earliestStartTime = Timestamp.valueOf("2099-12-31 23:59:59");
    for (Timestamp startTime : watchContainerStartTimeMap.values()) {
      earliestStartTime = earliestStartTime.before(startTime) ? earliestStartTime : startTime;
    }
    return earliestStartTime.equals(Timestamp.valueOf("2099-12-31 23:59:59")) ? Optional.empty() : Optional.of(earliestStartTime);
  }

  private Optional<Timestamp> getLatestQueryTimestamp() {
    Timestamp latestQueryTime = Timestamp.valueOf("1970-12-31 23:59:59");
    for (Timestamp lastTime : containerLastQueryTimeMap.values()) {
      latestQueryTime = latestQueryTime.after(lastTime) ? latestQueryTime : lastTime;
    }
    LOG.debug("DBlog's latest query timestamp :" + latestQueryTime.toString());
    return latestQueryTime.equals(Timestamp.valueOf("1970-12-31 23:59:59")) ? Optional.empty() : Optional.of(latestQueryTime);
  }

  public int getWatchRunningContainer(String appId) {
    Optional<WatchResult> watchResult = getWatchResult(appId);
    if (watchResult.isPresent()) {
      List<NodeStatusResult> nodeStatusResultList = watchResult.get().getNodesStatus();
      if (nodeStatusResultList != null) {
        int runningCount = 0;
        for (NodeStatusResult nodeStatus : nodeStatusResultList) {
          if ("RUNNING".equals(nodeStatus.getStatus())) {
            runningCount++;
          }
        }
        return runningCount;
      }
    }
    return 0;
  }

  public int getWatchTotalContainerNumber(String appId) {
    Optional<WatchResult> watchResult = getWatchResult(appId);
    if (watchResult.isPresent()) {
      List<NodeStatusResult> nodeStatusResultList = watchResult.get().getNodesStatus();
      if (nodeStatusResultList != null) {
        return nodeStatusResultList.size();
      }
    }
    return 0;
  }

  private void setWatchContainerStartTimeAndElaspsedTime(WatchResult watchResult) {
//              LOG.debug("setWatchContainerStartTimeAndElaspsedTime");
    if (watchResult != null) {
      List<NodeStatusResult> nodeStatusResultList = watchResult.getNodesStatus();
      if (nodeStatusResultList == null) {
        return;
      }
//        LOG.debug("nodeStatusResultList.size():"+nodeStatusResultList.size());
      for (NodeStatusResult nodeStatus : nodeStatusResultList) {
        String containerId = nodeStatus.getContainerID();
        watchContainerStartTimeMap.put(containerId, new Timestamp(nodeStatus.getStartTime()));
        watchContainerElapsedTimeMap.put(containerId, nodeStatus.getEndTime());
      }
    }
  }

  public void setRequestMemory(String appId, Long requestMemory) {
    if (requestMemory != null) {
      requestMemoryMap.put(appId, requestMemory);
    }
  }

  public void setRequestVcores(String appId, Long requestVcores) {
    if (requestVcores != null) {
      requestVcoresMap.put(appId, requestVcores);
    }
  }

  public Optional<Long> getRequestMemory(String appId) {
    if (requestMemoryMap.get(appId) != null) {
      return Optional.of(requestMemoryMap.get(appId));
    }
    return Optional.empty();
  }

  public Optional<Long> getRequestVcores(String appId) {
    if (requestVcoresMap.get(appId) != null) {
      return Optional.of(requestVcoresMap.get(appId));
    }
    return Optional.empty();
  }

}
