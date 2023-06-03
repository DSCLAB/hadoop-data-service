/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.yarn.application.containermanager;

import com.dslab.drs.monitor.ProcMemInfo;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 *
 * @author Weli
 */
public abstract class ContainerManager implements ResourcesManager {

  public void updateIntervals(int taskSize, ProcMemInfo procMeminfo) {
  }

  public Resource getContainerResource(String cID) {
    return null;
  }

  public ContainerInterval getContainerInterval(int memSize) {
    return null;
  }

  @Override
  public void release() {
  }

  @Override
  public void request() {
  }

  public void release(final String cID) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public void request(String host, boolean relaxLocality, int askcontainerMemory, int vcore) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public void turnOnResizeTrigger() {
  }

  public void setReleaseTime(long l) {
  }

  public void setRequestTime(long l) {
  }

  public long getReleaseTime() {
    long l = 0L;
    return l;
  }

  public long getRequestTime() {
    long l = 0L;
    return l;
  }

  public int getReleaseNum() {
    int i = 0;
    return i;
  }

  public int getRequestNum() {
    int i = 0;
    return i;
  }
}
