package com.dslab.drs.monitor;

import java.math.BigInteger;

public class CpuTimeTracker {

  public static final int UNAVAILABLE = -1;
  private final long minimumTimeInterval;

  // CPU used time since system is on (ms)
  private BigInteger cumulativeCpuTime = BigInteger.ZERO;

  // CPU used time read last time (ms)
  private BigInteger lastCumulativeCpuTime = BigInteger.ZERO;

  // Unix timestamp while reading the CPU time (ms)
  private long sampleTime;
  private long lastSampleTime;
  private float cpuUsage;
  private BigInteger jiffyLengthInMillis;

  public CpuTimeTracker(long jiffyLengthInMillis) {
    this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
    this.cpuUsage = UNAVAILABLE;
    this.sampleTime = UNAVAILABLE;
    this.lastSampleTime = UNAVAILABLE;
    minimumTimeInterval = 10 * jiffyLengthInMillis;
  }

  public float getCpuTrackerUsagePercent() {
    if (lastSampleTime == UNAVAILABLE
            || lastSampleTime > sampleTime) {
      // lastSampleTime > sampleTime may happen when the system time is changed
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
      return cpuUsage;
    }
    // When lastSampleTime is sufficiently old, update cpuUsage.
    // Also take a sample of the current time and cumulative CPU time for the
    // use of the next calculation.
    if (sampleTime > lastSampleTime + minimumTimeInterval) {
      cpuUsage = ((cumulativeCpuTime.subtract(lastCumulativeCpuTime)).floatValue())
              * 100F / ((float) (sampleTime - lastSampleTime));
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
    }

    return cpuUsage;
  }

  /**
   * Obtain the cumulative CPU time since the system is on.
   *
   * @return cumulative CPU time in milliseconds
   */
  public long getCumulativeCpuTime() {
    return cumulativeCpuTime.longValue();
  }

  /**
   * Apply delta to accumulators.
   *
   * @param elapsedJiffies updated jiffies
   * @param newTime new sample time
   */
  public void updateElapsedJiffies(BigInteger elapsedJiffies, long newTime) {
    cumulativeCpuTime = elapsedJiffies.multiply(jiffyLengthInMillis);
    sampleTime = newTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleTime ").append(this.sampleTime);
    sb.append(" CummulativeCpuTime ").append(this.cumulativeCpuTime);
    sb.append(" LastSampleTime ").append(this.lastSampleTime);
    sb.append(" LastCummulativeCpuTime ").append(this.lastCumulativeCpuTime);
    sb.append(" CpuUsage ").append(this.cpuUsage);
    sb.append(" JiffyLengthMillisec ").append(this.jiffyLengthInMillis);
    return sb.toString();
  }
}
