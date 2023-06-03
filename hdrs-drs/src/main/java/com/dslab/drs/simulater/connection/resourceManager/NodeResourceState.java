package com.dslab.drs.simulater.connection.resourceManager;

import java.util.Set;

/**
 *
 * @author kh87313
 */
public class NodeResourceState {

  final Set<String> nodes;
  final int availMemoryMB;
  final int availVcores;
  final int usedMemoryMB;
  final int usedVcores;

  public NodeResourceState(Set<String> nodes, int availMemoryMB, int availVcores, int usedMemoryMB, int usedVcores) {
    this.nodes = nodes;
    this.availMemoryMB = availMemoryMB;
    this.availVcores = availVcores;
    this.usedMemoryMB = usedMemoryMB;
    this.usedVcores = usedVcores;
  }

  public Set<String> getNodes() {
    return nodes;
  }

  public int getAvailMemoryMB() {
    return availMemoryMB;
  }

  public int getAvailVcores() {
    return availVcores;
  }

  public int getUsedMemoryMB() {
    return usedMemoryMB;
  }

  public int getUsedVcores() {
    return usedVcores;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    sb.append("availMemoryMB=").append(availMemoryMB).append(",");
    sb.append("availVcores=").append(availVcores).append(",");
    sb.append("usedMemoryMB=").append(usedMemoryMB).append(",");
    sb.append("usedVcores=").append(usedVcores).append("]");
    return sb.toString();
  }

}
