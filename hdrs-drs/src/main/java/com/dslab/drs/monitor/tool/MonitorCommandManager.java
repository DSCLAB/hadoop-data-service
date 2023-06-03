
package com.dslab.drs.monitor.tool;

/**
 *
 * @author kh87313
 */
public interface MonitorCommandManager {
  public void printRunningJobs();
  public void printJobStatus(String jobAppId);
  public void quit();
}
