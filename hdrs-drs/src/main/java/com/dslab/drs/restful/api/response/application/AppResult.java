package com.dslab.drs.restful.api.response.application;

import com.dslab.drs.restful.api.json.JsonSerialization;
import com.dslab.drs.restful.api.parser.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;

/**
 *
 * @author kh87313
 */
public class AppResult implements JsonSerialization {

  @VisibleForTesting
  String id;
  @VisibleForTesting
  String user;
  @VisibleForTesting
  String name;
  @VisibleForTesting
  String queue;
  @VisibleForTesting
  String state;
  @VisibleForTesting
  String finalStatus;
  @VisibleForTesting
  String progress;
  @VisibleForTesting
  String trackingUI;
  @VisibleForTesting
  String trackingUrl;
  @VisibleForTesting
  String diagnostics;
  @VisibleForTesting
  String clusterId;
  @VisibleForTesting
  String applicationType;
  @VisibleForTesting
  String applicationTags;
  @VisibleForTesting
  String finishedTime;
  @VisibleForTesting
  String elapsedTime;
  @VisibleForTesting
  String amContainerLogs;
  @VisibleForTesting
  String amHostHttpAddress;
  @VisibleForTesting
  String allocatedMB;
  @VisibleForTesting
  String allocatedVCores;
  @VisibleForTesting
  String runningContainers;
  @VisibleForTesting
  String memorySeconds;
  @VisibleForTesting
  String vcoreSeconds;
  @VisibleForTesting
  String preemptedResourceMB;
  @VisibleForTesting
  String preemptedResourceVCores;
  @VisibleForTesting
  String numAMContainerPreempted;
  @VisibleForTesting
  String logAggregationStatus;

  public String getId() {
    return id;
  }

  public String getUser() {
    return user;
  }

  public String getName() {
    return name;
  }

  public String getQueue() {
    return queue;
  }

  public String getState() {
    return state;
  }

  public String getFinalStatus() {
    return finalStatus;
  }

  public String getProgress() {
    return progress;
  }

  public String getTrackingUI() {
    return trackingUI;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getApplicationType() {
    return applicationType;
  }

  public String getApplicationTags() {
    return applicationTags;
  }

  public String getFinishedTime() {
    return finishedTime;
  }

  public String getElapsedTime() {
    return elapsedTime;
  }

  public String getAmContainerLogs() {
    return amContainerLogs;
  }

  public String getAmHostHttpAddress() {
    return amHostHttpAddress;
  }

  public String getAllocatedMB() {
    return allocatedMB;
  }

  public String getAllocatedVCores() {
    return allocatedVCores;
  }

  public String getRunningContainers() {
    return runningContainers;
  }

  public String getMemorySeconds() {
    return memorySeconds;
  }

  public String getVcoreSeconds() {
    return vcoreSeconds;
  }

  public String getPreemptedResourceMB() {
    return preemptedResourceMB;
  }

  public String getPreemptedResourceVCores() {
    return preemptedResourceVCores;
  }

  public String getNumAMContainerPreempted() {
    return numAMContainerPreempted;
  }

  public String getLogAggregationStatus() {
    return logAggregationStatus;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj instanceof AppResult) {
      AppResult dst = (AppResult) obj;
      return StringUtils.equals(id, dst.id)
              && StringUtils.equals(user, dst.user)
              && StringUtils.equals(name, dst.name)
              && StringUtils.equals(queue, dst.queue)
              && StringUtils.equals(state, dst.state)
              && StringUtils.equals(finalStatus, dst.finalStatus)
              && StringUtils.equals(progress, dst.progress)
              && StringUtils.equals(trackingUI, dst.trackingUI)
              && StringUtils.equals(trackingUrl, dst.trackingUrl)
              && StringUtils.equals(diagnostics, dst.diagnostics)
              && StringUtils.equals(clusterId, dst.clusterId)
              && StringUtils.equals(applicationType, dst.applicationType)
              && StringUtils.equals(applicationTags, dst.applicationTags)
              && StringUtils.equals(finishedTime, dst.finishedTime)
              && StringUtils.equals(elapsedTime, dst.elapsedTime)
              && StringUtils.equals(amContainerLogs, dst.amContainerLogs)
              && StringUtils.equals(amHostHttpAddress, dst.amHostHttpAddress)
              && StringUtils.equals(allocatedMB, dst.allocatedMB)
              && StringUtils.equals(allocatedVCores, dst.allocatedVCores)
              && StringUtils.equals(runningContainers, dst.runningContainers)
              && StringUtils.equals(memorySeconds, dst.memorySeconds)
              && StringUtils.equals(vcoreSeconds, dst.vcoreSeconds)
              && StringUtils.equals(preemptedResourceMB, dst.preemptedResourceMB)
              && StringUtils.equals(preemptedResourceVCores, dst.preemptedResourceVCores)
              && StringUtils.equals(numAMContainerPreempted, dst.numAMContainerPreempted)
              && StringUtils.equals(logAggregationStatus, dst.logAggregationStatus);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 29 * hash + Objects.hashCode(this.id);
    hash = 29 * hash + Objects.hashCode(this.user);
    hash = 29 * hash + Objects.hashCode(this.name);
    hash = 29 * hash + Objects.hashCode(this.queue);
    hash = 29 * hash + Objects.hashCode(this.state);
    hash = 29 * hash + Objects.hashCode(this.finalStatus);
    hash = 29 * hash + Objects.hashCode(this.progress);
    hash = 29 * hash + Objects.hashCode(this.trackingUI);
    hash = 29 * hash + Objects.hashCode(this.trackingUrl);
    hash = 29 * hash + Objects.hashCode(this.diagnostics);
    hash = 29 * hash + Objects.hashCode(this.clusterId);
    hash = 29 * hash + Objects.hashCode(this.applicationType);
    hash = 29 * hash + Objects.hashCode(this.applicationTags);
    hash = 29 * hash + Objects.hashCode(this.finishedTime);
    hash = 29 * hash + Objects.hashCode(this.elapsedTime);
    hash = 29 * hash + Objects.hashCode(this.amContainerLogs);
    hash = 29 * hash + Objects.hashCode(this.amHostHttpAddress);
    hash = 29 * hash + Objects.hashCode(this.allocatedMB);
    hash = 29 * hash + Objects.hashCode(this.allocatedVCores);
    hash = 29 * hash + Objects.hashCode(this.runningContainers);
    hash = 29 * hash + Objects.hashCode(this.memorySeconds);
    hash = 29 * hash + Objects.hashCode(this.vcoreSeconds);
    hash = 29 * hash + Objects.hashCode(this.preemptedResourceMB);
    hash = 29 * hash + Objects.hashCode(this.preemptedResourceVCores);
    hash = 29 * hash + Objects.hashCode(this.numAMContainerPreempted);
    hash = 29 * hash + Objects.hashCode(this.logAggregationStatus);

    return hash;
  }
}
