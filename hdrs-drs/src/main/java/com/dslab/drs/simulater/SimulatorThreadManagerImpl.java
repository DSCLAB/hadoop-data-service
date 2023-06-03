package com.dslab.drs.simulater;

import com.dslab.drs.utils.DrsConfiguration;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author kh87313
 */
public class SimulatorThreadManagerImpl implements SimulatorThreadManager {

  ExecutorService executor = Executors.newFixedThreadPool(15);
  HashMap<String, DrsThread> drsThreadMap = new HashMap<>();

  @Override
  public DrsThread newDrsThread(String id, DrsArguments drsArguments, DrsConfiguration drsConfiguration,boolean async) {
    return new DrsThread(id, drsArguments, drsConfiguration,async);
  }

  @Override
  public void runDrsThread(DrsThread drsThread) {
    String id = drsThread.getId();
    drsThreadMap.put(id, drsThread);
    executor.execute(drsThread);
  }


  @Override
  public void closeAll() {
    executor.shutdownNow();
  }
}
