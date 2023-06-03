package com.dslab.drs.socket;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class AppCliAskProgress extends DrsClientResponseState {

  private static final Log LOG = LogFactory.getLog(AppCliAskProgress.class);

  private final SocketRun sr;
  private final SocketGlobalVariables globals;

  AppCliAskProgress(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {
    try {
      if (globals.getScheduler().isComplete()) {
        LOG.info("Scheduler say completed.");
        sr.setDRSClientRunning(false);
      }
      updateTaskStatus(sr.getSocket(), globals);
      globals.getServiceStatus().setElapsed(System.currentTimeMillis() - globals.getServiceStatus().getStartTime());
    } catch (IOException ex) {
    }
  }

}
