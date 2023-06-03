/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.metric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.HDSConstants.LOADINGCATEGORY;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author jpopaholic
 */
public final class HDSMetricSystem implements MetricSystem {

  private final Map<LOADINGCATEGORY, AtomicMetricData> data;
  private final long ttl;
  private long flushStartTime;

  private static final Log LOG = LogFactory.getLog(HDSMetricSystem.class);

  private HDSMetricSystem(Configuration conf) {
    ttl = TimeUnit.MINUTES.toMillis(conf.getLong(
        HDSConstants.HDS_METRIC_HISTORY_TTL,
        HDSConstants.DEFAULT_METRIC_HISTORY_TTL));
    flushStartTime = System.currentTimeMillis();
    data = new HashMap(LOADINGCATEGORY.values().length);
    for (LOADINGCATEGORY key : LOADINGCATEGORY.values()) {
      data.put(key, new AtomicMetricData());
    }

  }

  public static ShareableObject<MetricSystem> getInstance(Configuration conf)
      throws Exception {
    return ShareableObject.<MetricSystem>create(
        (Object obj) -> obj instanceof MetricSystem,
        () -> new HDSMetricSystem(conf));
  }

  @Override
  public long getAllHistoryBytes(LOADINGCATEGORY type) {
    removeHistoryCounter();
    return data.get(type).getHistoryBytes();
  }

  @Override
  public int getAllHistoryRequests(LOADINGCATEGORY type) {
    removeHistoryCounter();
    return data.get(type).getHistoryCounters();
  }

  @Override
  public long getAllFutureBytes(LOADINGCATEGORY type) {
    removeHistoryCounter();
    return data.get(type).getFutureBytes();
  }

  @Override
  public int getAllFutureRequests(LOADINGCATEGORY type) {
    removeHistoryCounter();
    return data.get(type).getFutureCounters();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public MetricCounter registeExpectableCounter(LOADINGCATEGORY type, long expectBytes) {
    removeHistoryCounter();
    data.get(type).registerCounter(expectBytes);
    return new MetricCounter() {
      @Override
      public long getFutureBytes() {
        return expectBytes - countBytes;
      }

      @Override
      protected void internalAddBytes(long bytes) {
        data.get(type).addBytes(bytes, true);
      }

      @Override
      protected void internalClose() throws IOException {
        data.get(type).closeCounter(getFutureBytes());
      }

    };
  }

  @Override
  public MetricCounter registerUnExpectableCounter(LOADINGCATEGORY type) {
    removeHistoryCounter();
    data.get(type).registerCounter(0l);
    return new MetricCounter() {
      @Override
      public long getFutureBytes() {
        return 0l;
      }

      @Override
      protected void internalAddBytes(long bytes) {
        data.get(type).addBytes(bytes, false);
      }

      @Override
      protected void internalClose() throws IOException {
        data.get(type).closeCounter(getFutureBytes());
      }

    };
  }

  private void removeHistoryCounter() {
    long now = System.currentTimeMillis();
    if ((now - flushStartTime) > ttl) {
      for (LOADINGCATEGORY type : LOADINGCATEGORY.values()) {
        data.get(type).flushOld();
      }
      flushStartTime = now;
    }
  }

  private static class AtomicMetricData {

    private final AtomicLong historyBytes;
    private final AtomicLong futureBytes;
    private final AtomicInteger historyCounters;
    private final AtomicInteger futureCounters;

    public AtomicMetricData() {
      historyBytes = new AtomicLong(0l);
      futureBytes = new AtomicLong(0l);
      historyCounters = new AtomicInteger(0);
      futureCounters = new AtomicInteger(0);
    }

    public void registerCounter(long expectBytes) {
      futureCounters.incrementAndGet();
      futureBytes.addAndGet(expectBytes);
    }

    public void closeCounter(long failBytes) {
      futureBytes.accumulateAndGet(failBytes, (oldValue, num) -> oldValue - num);
      futureCounters.decrementAndGet();
      historyCounters.incrementAndGet();
    }

    public void addBytes(long bytes, boolean expectable) {
      historyBytes.addAndGet(bytes);
      if (expectable) {
        futureBytes.accumulateAndGet(bytes, (oldValue, num) -> oldValue - num);
      }
    }

    public long getHistoryBytes() {
      return historyBytes.get();
    }

    public long getFutureBytes() {
      return futureBytes.get();
    }

    public int getHistoryCounters() {
      return historyCounters.get();
    }

    public int getFutureCounters() {
      return futureCounters.get();
    }

    public void flushOld() {
      historyBytes.set(0l);
      historyCounters.set(0);
    }
  }

}
