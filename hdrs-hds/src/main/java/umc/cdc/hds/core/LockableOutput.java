/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.io.IOException;
import umc.cdc.hds.datastorage.DataStorageOutput;
import umc.cdc.hds.dblog.HdsLog;
import umc.cdc.hds.lock.Lock;
import umc.cdc.hds.tools.StopWatch;

/**
 *
 * @author brandboat
 */
public class LockableOutput extends DataStorageOutput {

  private final Lock lock;
  private final DataStorageOutput dsOutput;
  private final HdsLog hdsLog;

  public LockableOutput(Lock lock, DataStorageOutput dsOutput, HdsLog hdsLog) {
    this.lock = lock;
    this.dsOutput = dsOutput;
    this.hdsLog = hdsLog;
  }

  @Override
  public void close() throws IOException {
    try {
      StopWatch stopWatch = new StopWatch();
      dsOutput.close();
      if (hdsLog.getReleaseSourceElapsedTime().isPresent()) {
        hdsLog.setReleaseSourceElapsedTime(
            hdsLog.getReleaseSourceElapsedTime().get() + stopWatch.getElapsed());
      } else {
        hdsLog.setReleaseSourceElapsedTime(stopWatch.getElapsed());
      }
    } finally {
      StopWatch stopWatch = new StopWatch();
      lock.close();
      if (hdsLog.getReleaseLockElapsedTime().isPresent()) {
        hdsLog.setReleaseLockElapsedTime(
            hdsLog.getReleaseLockElapsedTime().get() + stopWatch.getElapsed());
      } else {
        hdsLog.setReleaseLockElapsedTime(stopWatch.getElapsed());
      }
    }
  }

  @Override
  public void recover() throws IOException {
    dsOutput.recover();
  }

  @Override
  public void write(int b) throws IOException {
    dsOutput.write(b);
  }

  @Override
  public void write(byte[] b, int offset, int length) throws IOException {
    dsOutput.write(b, offset, length);
  }

  @Override
  public long getSize() {
    return dsOutput.getSize();
  }

  @Override
  public String getName() {
    return dsOutput.getName();
  }

  @Override
  public String getUri() {
    return dsOutput.getUri();
  }

  @Override
  public void commit() throws IOException {
    dsOutput.commit();
  }

}
