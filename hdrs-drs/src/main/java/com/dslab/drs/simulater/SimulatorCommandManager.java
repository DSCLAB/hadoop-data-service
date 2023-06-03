package com.dslab.drs.simulater;

/**
 *
 * @author kh87313
 */
public interface SimulatorCommandManager {
//  1.start 2.stop 3.status 4.add 0.quit")
  public void init(SimulatorConfManager confManager);
  public void start();
  public void stop();
  public void addOne();
  public void killByAppId(String applicationId);
  public void printStatus();
  public void createFiles();
  public void clearLocalFiles();
  

}
