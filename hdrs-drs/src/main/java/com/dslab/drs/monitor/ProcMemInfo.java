/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.monitor;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Weli
 */
public class ProcMemInfo implements java.io.Serializable {

  private static ProcMemInfo INSTANCE;
  //container size->MB, memeory ->byte
  private String containerID;
  private long memory = 0L;
  private long totalMem = 0L;
  private int updateCount = 0;
  private final List<Long> memories = new ArrayList<>();

  private ProcMemInfo() {
  }

  public static ProcMemInfo getInstance() {
    if (INSTANCE == null) {
      synchronized (ProcMemInfo.class) {
        if (INSTANCE == null) {
          INSTANCE = new ProcMemInfo();
        }
      }
    }
    return INSTANCE;
  }

  public void updateInfo(long memory) {
    this.memories.add(memory);
    this.memory = memory;
    this.totalMem += memory;
    this.updateCount++;
  }

  public void reset() {
    this.totalMem = 0L;
    this.updateCount = 0;
    this.memories.clear();
  }

  public long getMem() {
    return this.memory;
  }

  public long getAVGMem() {
    if (this.updateCount == 0) {
      return this.totalMem;
    }
    return this.totalMem / this.updateCount;
  }

  public ProcMemInfo setContainerID(String containerID) {
    this.containerID = containerID;
    return this;
  }

  public String getContainerID() {
    return this.containerID;
  }

  public void outPutMemLog() {
    System.out.println(containerID + ",size =" + memories.size() + " Mem = ");
    memories.stream().forEach(m -> System.out.print(m + "->"));
    System.out.println();
  }

}
