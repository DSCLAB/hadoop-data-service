/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.metric;

import umc.udp.core.framework.AtomicCloseable;

/**
 *
 * @author jpopaholic
 */
public abstract class MetricCounter extends AtomicCloseable {

  protected long countBytes;

  public MetricCounter() {
    countBytes = 0l;
  }

  public void addBytes(long bytes) {
    if (this.isClosed()) {
      throw new RuntimeException("Counter is closed! Cannot count anymore");
    }
    countBytes += bytes;
    internalAddBytes(bytes);
  }

  protected abstract void internalAddBytes(long bytes);

  public abstract long getFutureBytes();

}
