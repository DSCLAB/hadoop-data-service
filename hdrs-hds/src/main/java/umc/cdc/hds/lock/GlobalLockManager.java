/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.lock;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public interface GlobalLockManager extends Closeable {

  /**
   * get read file lock from system
   *
   * @param uri the source
   * @return the lock user can use or close lock.if someone is holding lock,user
   * will get optional.empty()
   * @throws java.io.IOException if faild to get lock ,it throw
   */
  Optional<Lock> getReadLock(UriRequest uri) throws IOException;

  /**
   * get write file lock from system
   *
   * @param uri the source
   * @return the lock user can use or close lock.if someone is holding lock,user
   * will get optional.empty()
   * @throws java.io.IOException if faild to get lock ,it throw
   */
  Optional<Lock> getWriteLock(UriRequest uri) throws IOException;

  /**
   * get read lock amount.
   *
   * @return lock amount.
   */
  long getReadLockNum();

  /**
   * get write lock amount.
   *
   * @return
   */
  long getWriteLockNum();

}
