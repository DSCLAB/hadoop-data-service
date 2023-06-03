package com.dslab.drs.utils;

public class DrsSystemUtils {
    public static String getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return processName.split("@")[0];
  }
}
