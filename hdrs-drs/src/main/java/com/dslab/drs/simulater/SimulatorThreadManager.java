package com.dslab.drs.simulater;

import com.dslab.drs.utils.DrsConfiguration;

/**
 *
 * @author kh87313
 */
public interface SimulatorThreadManager {
  public DrsThread newDrsThread(String id,DrsArguments drsArguments,DrsConfiguration drsConfiguration,boolean async);
  public void runDrsThread(DrsThread drsThread);
  public void closeAll();
}
