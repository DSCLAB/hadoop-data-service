/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.lock;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HDSConstants;

/**
 *
 * @author jpopaholic
 */
public abstract class GlobalLockManagerFactory {

  private static volatile GlobalLockManager manager;
  private static final Object OBJ = new Object();

  public static GlobalLockManager createGlobalLockManager(Configuration conf) throws IOException {
    synchronized (OBJ) {
      if (manager == null) {
        manager = conf.getBoolean(HDSConstants.GLOBAL_LOCK_DISABLE, false)
            ? new NullableLockManager()
            : new HDSLockManager(conf);
      }
      return manager;
    }
  }
}
