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
public interface Logger extends AutoCloseable {

  public void addLog(LogCell cell);
}
