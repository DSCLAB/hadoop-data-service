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
public final class LogIntegrations {

  private final StringBuilder IntervalSizeArray = new StringBuilder();
  private final StringBuilder nowContainerNum = new StringBuilder();
  private final StringBuilder fileCount = new StringBuilder();
  private final StringBuilder taskAVGtime = new StringBuilder();
  private final StringBuilder goalContainerNum = new StringBuilder();
  private final StringBuilder requestContainerNum = new StringBuilder();
  private final StringBuilder killContainerNum = new StringBuilder();

  public LogIntegrations appendIntervalSize(int size) {
    IntervalSizeArray.append(size).append(", ");
    return this;
  }

  public LogIntegrations appendIntervalSize(String size) {
    IntervalSizeArray.append(size).append(", ");
    return this;
  }

  public LogIntegrations appendNowContainerNum(int num) {
    nowContainerNum.append(num);
    return this;
  }

  public LogIntegrations appendNowContainerNum(String num) {
    nowContainerNum.append(num);
    return this;
  }

  public LogIntegrations appendFileCount(long num) {
    fileCount.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendFileCount(String num) {
    fileCount.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendTaskAVGtime(long num) {
    taskAVGtime.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendTaskAVGtime(String num) {
    taskAVGtime.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendGoalContainerNum(long num) {
    goalContainerNum.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendGoalContainerNum(String num) {
    goalContainerNum.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendRequestContainerNum(long num) {
    requestContainerNum.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendRequestContainerNum(String num) {
    requestContainerNum.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendKillContainerNum(long num) {
    killContainerNum.append(num).append(", ");
    return this;
  }

  public LogIntegrations appendKillContainerNum(String num) {
    killContainerNum.append(num).append(", ");
    return this;
  }

  public String getIntervalSizeLog() {
    IntervalSizeArray.replace(IntervalSizeArray.length() - 2, IntervalSizeArray.length(), "]");
    return IntervalSizeArray.toString();
  }

  public String getNowContainerNumLog() {
    nowContainerNum.replace(nowContainerNum.length() - 2, nowContainerNum.length(), "]");
    return nowContainerNum.toString();
  }

  public String getFileCount() {
    fileCount.replace(fileCount.length() - 2, fileCount.length(), "]");
    return fileCount.toString();
  }

  public String getTaskAVGtime() {
    taskAVGtime.replace(taskAVGtime.length() - 2, taskAVGtime.length(), "]");
    return taskAVGtime.toString();
  }

  public String getGoalContainerNumLog() {
    goalContainerNum.replace(goalContainerNum.length() - 2, goalContainerNum.length(), "]");
    return goalContainerNum.toString();
  }

  public String getRequestContainerNumLog() {
    requestContainerNum.replace(requestContainerNum.length() - 2, requestContainerNum.length(), "]");
    return requestContainerNum.toString();
  }

  public String getKillContainerNumLog() {
    killContainerNum.replace(killContainerNum.length() - 2, killContainerNum.length(), "]");
    return killContainerNum.toString();
  }

  public void reset() {
    IntervalSizeArray.delete(0, IntervalSizeArray.length());
    nowContainerNum.delete(0, nowContainerNum.length());
    fileCount.delete(0, fileCount.length());
    taskAVGtime.delete(0, taskAVGtime.length());
    goalContainerNum.delete(0, goalContainerNum.length());
    requestContainerNum.delete(0, requestContainerNum.length());
    killContainerNum.delete(0, killContainerNum.length());
  }

  public void init() {
    IntervalSizeArray.append("[");
    nowContainerNum.append("[");
    fileCount.append("[");
    taskAVGtime.append("[");
    goalContainerNum.append("[");
    requestContainerNum.append("[");
    killContainerNum.append("[");
  }

}
