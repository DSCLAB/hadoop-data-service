package com.dslab.drs.drslog;

/**
 *
 * @author kh87313
 */
public interface LogClientManager {

  public void setContainerID(String containerID);

  public void startResourceMonitor();

  public void stopResourceMonitor();

  public void startLogClientSocket();

  public void stopLogClientSocket();

  public void startClientResizeMonitor();

  public void stopClientResizeMonitor();

  public void shutDown();
  
  public void startMonitor();
  
  public void stopMonitor();

}
