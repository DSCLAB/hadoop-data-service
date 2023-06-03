/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Metrics Helper Utils.
 *
 * @author brandboat
 */
public class HdsMetricsHelper {

  public static MetricsInfo info(String name, String desc) {
    return new MetricsInfoImpl(name, desc);
  }
}
