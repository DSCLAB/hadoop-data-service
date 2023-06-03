/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.umc.hdrs.dblog;

/**
 *
 * @author brandboat
 */
public class EmptyDbLogger implements Logger {

  public static final EmptyDbLogger DBLOGGER = new EmptyDbLogger();

  public static EmptyDbLogger getDbLogger() {
    return DBLOGGER;
  }

  @Override
  public void addLog(LogCell cell) {
  }

  @Override
  public void close() throws Exception {
  }
}
