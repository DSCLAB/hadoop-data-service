/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.metrics2.lib;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.lib.HdsMetricsSource.Rate;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.httpserver.HdsHttpServer;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public class HdsMetrics implements AutoCloseable {

  private HdsMetricsWrapper hdsWrapper;
  private HdsMetricsSourceImpl hdsSource;

  public static ShareableObject<HdsMetrics> getInstance(HdsHttpServer hds)
      throws Exception {
    return ShareableObject.<HdsMetrics>create(
        (o) -> o instanceof HdsMetrics,
        () -> new HdsMetrics(new HdsMetricsWrapperImpl(hds)));
  }

  private HdsMetrics(HdsMetricsWrapper hdsWrapper) throws Exception {
    this(hdsWrapper, HdsMetricsSourceImpl.getInstance(hdsWrapper));
  }

  HdsMetrics(HdsMetricsWrapper hdsWrapper,
      HdsMetricsSourceImpl hdsSource) {
    this.hdsWrapper = hdsWrapper;
    this.hdsSource = hdsSource;
  }

  public void updateReadWriteRate(Protocol p, Rate r, long val) {
    hdsSource.updateReadWriteRate(p, r, val);
  }

  @VisibleForTesting
  public HdsMetricsSourceImpl getHdsSource() {
    return hdsSource;
  }

  public HdsMetricsWrapper getHdsWrapper() {
    return hdsWrapper;
  }

  @Override
  public void close() throws Exception {
//        hdsSource.close();
  }
}
