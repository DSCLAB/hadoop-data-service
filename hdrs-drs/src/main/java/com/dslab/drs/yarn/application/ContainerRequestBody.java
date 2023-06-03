package com.dslab.drs.yarn.application;

/**
 *
 * @author chen10
 */
public class ContainerRequestBody {

  private String nodeHostName;
  private int memoryMB;

  public ContainerRequestBody(String nodeHostName, int memoryMB) {
    this.nodeHostName = nodeHostName;
    this.memoryMB = memoryMB;
  }

  public String getNodeHostName() {
    return this.nodeHostName;
  }

  public int getMemoryMB() {
    return this.memoryMB;
  }
}
