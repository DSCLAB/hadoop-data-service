package com.dslab.drs.drslog;

import com.dslab.drs.exception.DrsLoggerException;

/**
 *
 * @author kh87313
 */
public class LogServerManagerEmpty  implements LogServerManager {

  @Override
  public void startResourceMonitor() {
      //Do nothing
  }

  @Override
  public void stopResourceMonitor() {
      //Do nothing
  }

  @Override
  public void addPhaseLog(DrsPhaseLog cell) {
      //Do nothing
  }

  @Override
  public void addResourceLog(DrsResourceLog cell) {
      //Do nothing
  }

  @Override
  public void addLogPacket(LogPacket packet) {
      //Do nothing
  }

  @Override
  public void shutDown() {
      //Do nothing
  }

  @Override
  public void createDblogTable() throws DrsLoggerException {
      //Do nothing 
  }

  @Override
  public int getPhaseLogCount() {
      return 0;
  }

  @Override
  public int getResourceLogCount() {
      return 0;
  }
  
}
