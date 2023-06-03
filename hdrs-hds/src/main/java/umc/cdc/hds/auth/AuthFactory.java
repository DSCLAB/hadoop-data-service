/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import org.apache.hadoop.conf.Configuration;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public final class AuthFactory {

  private AuthFactory() {
  }

  public static ShareableObject<LocalAuth> getLocalAuth(Configuration conf)
      throws Exception {
    return ShareableObject.<LocalAuth>create(
        (Object obj) -> obj instanceof LocalAuth,
        () -> new LocalAuth(conf));
  }

  public static ShareableObject<HdfsAuth> getHdfsAuth(Configuration conf)
      throws Exception {
    return ShareableObject.<HdfsAuth>create(
        (Object obj) -> obj instanceof HdfsAuth,
        () -> new HdfsAuth(conf));
  }
}
