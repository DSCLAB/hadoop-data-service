package com.dslab.drs.restful.api.response.node;

import com.dslab.drs.restful.api.parser.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import com.dslab.drs.restful.api.json.JsonSerialization;

public class NodeResult implements JsonSerialization {

  @VisibleForTesting
  String rack;
  @VisibleForTesting
  String state;
  @VisibleForTesting
  String id;
  @VisibleForTesting
  String nodeHostName;
  @VisibleForTesting
  String nodeHTTPAddress;
  @VisibleForTesting
  String lastHealthUpdate;
  @VisibleForTesting
  String version;
  @VisibleForTesting
  String healthReport;
  @VisibleForTesting
  int numContainers;
  @VisibleForTesting
  int usedMemoryMB;
  @VisibleForTesting
  int availMemoryMB;
  @VisibleForTesting
  int usedVirtualCores;
  @VisibleForTesting
  int availableVirtualCores;

  public String getRack() {
    return this.rack;
  }

  public String getState() {
    return this.state;
  }

  public String getId() {
    return this.id;
  }

  public String getNodeHostName() {
    return this.nodeHostName;
  }

  public String getNodeHTTPAddress() {
    return this.nodeHTTPAddress;
  }

  public String getLastHealthUpdate() {
    return this.lastHealthUpdate;
  }

  public String getVersion() {
    return this.version;
  }

  public String getHealthReport() {
    return this.healthReport;
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  public int getUsedMemoryMB() {
    return this.usedMemoryMB;
  }

  public int getAvailMemoryMB() {
    return this.availMemoryMB;
  }

  public int getUsedVirtualCores() {
    return this.usedVirtualCores;
  }

  public int getAvailableVirtualCores() {
    return this.availableVirtualCores;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj instanceof NodeResult) {
      NodeResult dst = (NodeResult) obj;
      return numContainers == dst.numContainers
          && usedMemoryMB == dst.usedMemoryMB
          && availMemoryMB == dst.availMemoryMB
          && usedVirtualCores == dst.usedVirtualCores
          && availableVirtualCores == dst.availableVirtualCores
          && StringUtils.equals(id, dst.id)
          && StringUtils.equals(rack, dst.rack)
          && StringUtils.equals(state, dst.state)
          && StringUtils.equals(nodeHTTPAddress, dst.nodeHTTPAddress)
          && StringUtils.equals(nodeHostName, dst.nodeHostName)
          && StringUtils.equals(lastHealthUpdate, dst.lastHealthUpdate)
          && StringUtils.equals(version, dst.version)
          && StringUtils.equals(healthReport, dst.healthReport);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 29 * hash + Objects.hashCode(this.rack);
    hash = 29 * hash + Objects.hashCode(this.state);
    hash = 29 * hash + Objects.hashCode(this.id);
    hash = 29 * hash + Objects.hashCode(this.nodeHostName);
    hash = 29 * hash + Objects.hashCode(this.nodeHTTPAddress);
    hash = 29 * hash + Objects.hashCode(this.lastHealthUpdate);
    hash = 29 * hash + Objects.hashCode(this.version);
    hash = 29 * hash + Objects.hashCode(this.healthReport);
    hash = 29 * hash + this.numContainers;
    hash = 29 * hash + this.usedMemoryMB;
    hash = 29 * hash + this.availMemoryMB;
    hash = 29 * hash + this.usedVirtualCores;
    hash = 29 * hash + this.availableVirtualCores;
    return hash;
  }
}
