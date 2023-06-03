/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsInfo;

/**
 *
 * @author brandboat
 */
public class HdsMetricsInfoImpl implements MetricsInfo {

  private final String metricsName;
  private final String metricsDesc;

  public HdsMetricsInfoImpl(String metricsName, String metricsDesc) {
    this.metricsName = metricsName;
    this.metricsDesc = metricsDesc;
  }

  @Override
  public String name() {
    return metricsName;
  }

  @Override
  public String description() {
    return metricsDesc;
  }

}
