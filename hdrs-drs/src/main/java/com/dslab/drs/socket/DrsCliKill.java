/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.socket;

import java.io.IOException;

/**
 *
 * @author Weli
 */
public class DrsCliKill extends DrsClientResponseState {

  private final SocketRun sr;
  private final SocketGlobalVariables globals;

  DrsCliKill(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {
    try {
      globals.setIsRunning(false);
      globals.setIsKilled(true);
      updateTaskStatus(sr.getSocket(), globals);
    } catch (IOException ex) {
    }
  }

}
