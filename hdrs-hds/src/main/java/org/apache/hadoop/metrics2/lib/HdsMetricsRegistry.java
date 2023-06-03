/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;

/**
 *
 * @author brandboat
 */
public class HdsMetricsRegistry {

  private final ConcurrentMap<String, MutableMetric> metricMap
      = Maps.newConcurrentMap();
  private final ConcurrentMap<String, MetricsTag> tagsMap
      = Maps.newConcurrentMap();
  private final MetricsInfo metricsInfo;

  public HdsMetricsRegistry(String name) {
    this(HdsMetricsHelper.info(name, name));
  }

  public HdsMetricsRegistry(MetricsInfo info) {
    metricsInfo = info;
  }

  public MetricsInfo info() {
    return metricsInfo;
  }

  public HdsMetricsRegistry tag(MetricsInfo info, String value, boolean override) {
    MetricsTag tag = Interns.tag(info, value);

    if (!override) {
      MetricsTag existing = tagsMap.putIfAbsent(info.name(), tag);
      if (existing != null) {
        throw new MetricsException("Tag " + info.name() + " already exists!");
      }
      return this;
    }

    tagsMap.put(info.name(), tag);

    return this;
  }

  public HdsMetricsRegistry tag(MetricsInfo info, String value) {
    return tag(info, value, false);
  }

  public HdsMetricsRegistry tag(String name, String description, String value) {
    return tag(name, description, value, false);
  }

  public HdsMetricsRegistry tag(String name, String description, String value,
      boolean override) {
    return tag(new HdsMetricsInfoImpl(name, description), value, override);
  }

  /**
   * Set the metrics context tag
   *
   * @param name of the context
   * @return the registry itself as a convenience
   */
  public HdsMetricsRegistry setContext(String name) {
    return tag(MsInfo.Context, name, true);
  }

  /**
   * Get gauge long from registy, if not return null.
   *
   * @param gaugeName
   * @return MutableGaugeLong
   */
  public MutableGaugeLong getGauge(String gaugeName) {
    MutableMetric metric = metricMap.get(gaugeName);
    if (metric == null) {
      return null;
    }
    if (!(metric instanceof MutableGaugeLong)) {
      throw new MetricsException("Metric already exists in registry for metric name: "
          + gaugeName + " and not of type MutableCounter");
    }
    return (MutableGaugeLong) metric;
  }

  /**
   * Get MutableMetric from registry.
   *
   * @param name
   * @return
   */
  public MutableMetric get(String name) {
    return metricMap.get(name);
  }

  public MutableGaugeLong newGaugeLong(String name, String desc, long val) {
    return newGaugeLong(HdsMetricsHelper.info(name, desc), val);
  }

  public MutableGaugeLong newGaugeLong(MetricsInfo info, long val) {
    MutableGaugeLong newGauge = new MutableGaugeLong(info, val);
    return addMutableMetricIfNotPresent(
        info.name(), newGauge, MutableGaugeLong.class);
  }

  public void clear() {
    metricMap.clear();
  }

  Collection<MetricsTag> tags() {
    return tagsMap.values();
  }

  Collection<MutableMetric> metrics() {
    return metricMap.values();
  }

  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    for (MetricsTag tag : tags()) {
      builder.add(tag);
    }
    for (MutableMetric metric : metrics()) {
      metric.snapshot(builder, all);
    }
  }

  private <T extends MutableMetric> T addMutableMetricIfNotPresent(
      String name, T newMetric, Class<T> metricClass) {
    MutableMetric metric = metricMap.putIfAbsent(name, newMetric);
    if (metric == null) {
      return newMetric;
    }
    // if metricClass is a father of newMetric.
    if (!metricClass.isAssignableFrom(newMetric.getClass())) {
      throw new MetricsException("Metric already exists in registry for metric name: "
          + name + " and not of type " + metricClass
          + " but instead of type " + metric.getClass());
    }
    return (T) metric;
  }

}
