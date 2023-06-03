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
public class WrongReq extends ContainerResponseState {

  private final SocketRun sr;
  private final SocketGlobalVariables globals;

  WrongReq(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {
    try {
      ServerToClientMessage message = new ServerToClientMessage("WrongRequest");
      outputMessage(message, sr.getSocket());
    } catch (IOException ex) {
    }
  }

}
