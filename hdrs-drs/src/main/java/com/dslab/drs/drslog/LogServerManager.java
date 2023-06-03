package com.dslab.drs.drslog;

import com.dslab.drs.exception.DrsLoggerException;

/**
 *
 * @author kh87313
 */
public interface LogServerManager {

  public void startResourceMonitor();

  public void stopResourceMonitor();

  public void createDblogTable() throws DrsLoggerException;

  public void addPhaseLog(DrsPhaseLog cell);

  public void addResourceLog(DrsResourceLog cell);

  public void addLogPacket(LogPacket packet);

  public void shutDown();

  public int getPhaseLogCount();

  public int getResourceLogCount();
}
