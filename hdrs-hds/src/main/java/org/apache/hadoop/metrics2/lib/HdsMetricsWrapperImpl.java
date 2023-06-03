/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HDS;
import umc.cdc.hds.httpserver.HdsHttpServer;

/**
 *
 * @author brandboat
 */
public class HdsMetricsWrapperImpl implements HdsMetricsWrapper {

  // don't close httpserver here
  private final HdsHttpServer hds;

  public HdsMetricsWrapperImpl(HdsHttpServer hds) {
    this.hds = hds;
  }

  @Override
  public long getLongTaskNum() {
    return hds.getLongTaskNum();
  }

  @Override
  public long getShortTaskNum() {
    return hds.getShortTaskNum();
  }

  @Override
  public long getReadLockNum() {
    return HDS.getReadLockNum();
  }

  @Override
  public long getWriteLockNum() {
    return HDS.getWriteLockNum();
  }

  @Override
  public long getFtpConnNum() {
    return hds.getFtpConnNum();
  }

  @Override
  public long getJdbcConnNum() {
    return hds.getJdbcConnNum();
  }

  @Override
  public Configuration getConfig() {
    return hds.getConfig();
  }

  @Override
  public long getSharedMemTotal() {
    return HDS.getSharedMemTotal();
  }

  @Override
  public long getSharedMemTaken() {
    return HDS.getSharedMemTaken();
  }

}
