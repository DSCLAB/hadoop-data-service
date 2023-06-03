/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import umc.cdc.hds.core.Configurable;

/**
 *
 * @author brandboat
 */
public interface HdsMetricsWrapper extends Configurable {

  long getLongTaskNum();

  long getShortTaskNum();

  long getReadLockNum();

  long getWriteLockNum();

  long getFtpConnNum();

  long getJdbcConnNum();

  long getSharedMemTotal();

  long getSharedMemTaken();
}
