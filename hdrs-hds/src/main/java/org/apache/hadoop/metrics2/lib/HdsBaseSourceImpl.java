/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;

/**
 *
 * @author brandboat
 */
public class HdsBaseSourceImpl implements HdsBaseSource, MetricsSource {

  private static enum DefaultMetricsSystemInitializer {
    INSTANCE;
    private boolean inited = false;

    synchronized void init(String name) {
      if (inited) {
        return;
      }
      inited = true;
      DefaultMetricsSystem.initialize(HDS_METRICS_SYSTEM_NAME);
    }
  }

  protected HdsMetricsRegistry metricsRegistry;
  protected String metricsName;
  protected String metricsDesc;
  protected String metricsContext;
  protected String metricsJmxContext;

  public HdsBaseSourceImpl(
      String metricsName,
      String metricsDesc,
      String metricsContext,
      String metricsJmxContext) {
    this.metricsName = metricsName;
    this.metricsDesc = metricsDesc;
    this.metricsContext = metricsContext;
    this.metricsJmxContext = metricsJmxContext;

    metricsRegistry = new HdsMetricsRegistry(metricsName).setContext(metricsContext);
    init();
    DefaultMetricsSystemInitializer.INSTANCE.init(metricsName);
    DefaultMetricsSystem.instance().register(metricsJmxContext, metricsDesc, this);
  }

  private void init() {
    metricsRegistry.clear();
  }

  @Override
  public void setGauge(String gaugeName, String gaugeDesc, long val) {
    metricsRegistry.newGaugeLong(new HdsMetricsInfoImpl(gaugeName, gaugeDesc), val);
  }

  @Override
  public void incGauge(String gaugeName, long val) {
    MutableGaugeLong gauge = metricsRegistry.getGauge(gaugeName);
    gauge.incr(val);
  }

  @Override
  public void decGauge(String gaugeName, long val) {
    MutableGaugeLong gauge = metricsRegistry.getGauge(gaugeName);
    gauge.incr(val);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {

  }

}
