package com.dslab.drs.utils;

/**
 *
 * @author caca
 */
public class ProcessTimer {

  private long millsSec = 0;
  private static final ProcessTimer timer = new ProcessTimer();

  public static ProcessTimer getTimer() {
    return timer;
  }

  public void settime() {
    millsSec = System.currentTimeMillis();
  }

  public long getDifferenceMillis() {
    long temp = millsSec;
    millsSec = System.currentTimeMillis();
    return millsSec - temp;
  }

  public long getLastTime() {
    return millsSec;
  }
}
