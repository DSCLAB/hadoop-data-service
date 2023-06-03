/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author brandboat
 */
public class StopWatch {

  long startTime = 0;
  long stopTime = 0;

  public StopWatch() {
    startTime = System.nanoTime();
  }

  public long getElapsed() {
    stopTime = System.nanoTime();
    return TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
  }
}
