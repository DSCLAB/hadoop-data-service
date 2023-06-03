/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.socket;

import com.dslab.drs.yarn.application.ServerToClientMessage;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class SchedulerSuccess extends ContainerResponseState {

  private final SocketRun sr;
  private final SocketGlobalVariables globals;
  private static final Log LOG = LogFactory.getLog(SchedulerSuccess.class);

  SchedulerSuccess(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {
    String containerID = sr.getAsk()[2];
    try {
      ServerToClientMessage message = new ServerToClientMessage("Finish");
      LOG.info("containerID:" + containerID + " finish -> close!");
//      globals.decreaseAMExpectedContainer();
      globals.addReleasedContainerID(containerID);
      super.outputMessage(message, sr.getSocket());
      sr.increaseContainerFinishCount();
    } catch (IOException ex) {
    }
  }

}
