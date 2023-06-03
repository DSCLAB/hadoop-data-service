/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.lock;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class NullableLockManager implements GlobalLockManager {

  private Optional<Lock> getLock(UriRequest uri) throws IOException {
    return Optional.of(new Lock() {
      @Override
      public byte[] getKey() {
        return Bytes.toBytes(uri.toRealUri());
      }

      @Override
      protected void internalClose() throws IOException {

      }
    });
  }

  @Override
  public Optional<Lock> getReadLock(UriRequest uri) throws IOException {
    return getLock(uri);
  }

  @Override
  public Optional<Lock> getWriteLock(UriRequest uri) throws IOException {
    return getLock(uri);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public long getReadLockNum() {
    return 0;
  }

  @Override
  public long getWriteLockNum() {
    return 0;
  }

}
