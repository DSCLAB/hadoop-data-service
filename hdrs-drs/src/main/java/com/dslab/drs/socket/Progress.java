/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.socket;

import com.dslab.drs.yarn.application.ServerToClientMessage;
import java.io.IOException;

/**
 *
 * @author Weli
 */
public class Progress extends ContainerResponseState {

  private final SocketRun sr;
  private final SocketGlobalVariables globals;

  Progress(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {
    try {
      int progress = globals.getScheduler().getProgress();
      ServerToClientMessage message = new ServerToClientMessage("Progress", progress);
      outputMessage(message, sr.getSocket());
    } catch (IOException ex) {
    }
  }

}
