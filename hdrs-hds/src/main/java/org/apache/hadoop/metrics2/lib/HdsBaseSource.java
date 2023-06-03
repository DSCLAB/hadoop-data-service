/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

/**
 *
 * @author brandboat
 */
public interface HdsBaseSource {

  public static final String HDS_METRICS_SYSTEM_NAME = "HDS";

  public void setGauge(String gaugeName, String gaugeDesc, long val);

  public void incGauge(String gaugeName, long val);

  public void decGauge(String gaugeName, long val);

}
