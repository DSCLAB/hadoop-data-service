/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.yarn.application.containermanager;

/**
 *
 * @author Weli
 */
public class ResizeElement {

  private int fileNum = 0;
  private long taskTime = 0;
  private int totoalFileCount = 0;

  public ResizeElement setFileNum(int fileNum) {
    this.fileNum = fileNum;
    return this;
  }

  public ResizeElement setTaskTime(long taskTime) {
    this.taskTime = taskTime;
    return this;
  }

  public ResizeElement setTotalFileNum(int fileNum) {
    this.totoalFileCount = fileNum;
    return this;
  }

  public int getFileNum() {
    return fileNum;
  }

  public long getTaskTime() {
    return taskTime;
  }

  public int getTotalFileNum() {
    return totoalFileCount;
  }

}
