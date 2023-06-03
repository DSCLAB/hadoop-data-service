/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.io.IOException;
import umc.cdc.hds.datastorage.DataStorageInput;
import umc.cdc.hds.dblog.HdsLog;
import umc.cdc.hds.lock.Lock;
import umc.cdc.hds.tools.StopWatch;

/**
 *
 * @author brandboat
 */
public class LockableInput extends DataStorageInput {

  private final Lock lock;
  private final DataStorageInput dsInput;
  private final HdsLog hdsLog;

  public LockableInput(Lock lock, DataStorageInput dsInput, HdsLog hdsLog) {
    this.lock = lock;
    this.dsInput = dsInput;
    this.hdsLog = hdsLog;
  }

  @Override
  public void close() throws IOException {
    try {
      StopWatch stopWatch = new StopWatch();
      dsInput.close();
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
  public int read() throws IOException {
    return dsInput.read();
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    return dsInput.read(b, offset, length);
  }

  @Override
  public void recover() {
    dsInput.recover();
  }

  @Override
  public long getSize() {
    return dsInput.getSize();
  }

  @Override
  public String getName() {
    return dsInput.getName();
  }

  @Override
  public String getUri() {
    return dsInput.getUri();
  }

}
