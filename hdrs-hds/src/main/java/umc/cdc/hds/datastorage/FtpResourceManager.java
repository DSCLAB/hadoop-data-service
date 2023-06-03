/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.net.ftp.FTPClient;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.exceptions.FtpResourceException;
import umc.cdc.hds.mapping.AccountInfo;
import umc.cdc.hds.uri.UriRequest;
import umc.udp.core.framework.RestrictedListMap;
import umc.udp.core.framework.RestrictedListMap.Element;

/**
 *
 * @author jpopaholic
 */
public class FtpResourceManager implements Closeable {

  private final long timeout;
  private final RestrictedListMap<FtpClientInfo, FtpClientValue> resource;

  public FtpResourceManager(int poolSize, int checkTime, long timeout) {
    resource = new RestrictedListMap<>(poolSize, checkTime);
    this.timeout = timeout;
  }

  public FtpResource getFtpResource(UriRequest uri) throws IOException {
    FtpClientInfo info = new FtpClientInfo(uri);
    return new FtpResource(resource.get(info, createBuilder(info)));
  }

  @Override
  public void close() throws IOException {
    resource.close();
  }

  private class FtpClientInfo implements Comparable<FtpClientInfo> {

    private final AccountInfo info;

    public FtpClientInfo(UriRequest uri) throws IOException {
      this.info = uri.getAccountInfo().orElseThrow(() -> new FtpResourceException(HDSConstants.NO_HOST));
      if (!info.getHost().isPresent()) {
        throw new FtpResourceException(HDSConstants.NO_HOST);
      }
    }

    @Override
    public int compareTo(FtpClientInfo o) {
      if (info.getHost().get().equals(o.info.getHost().get())) {
        if (info.getId().equals(o.info.getId())) {
          if (info.getPasswd().equals(o.info.getPasswd())) {
            return 0;
          } else {
            return -1;
          }
        } else {
          return -1;
        }
      }
      return -1;
    }

    private String getInfo() {
      return info.toRealString();
    }
  }

  private class FtpClientValue implements RestrictedListMap.Value {

    private final FTPClient client;
    private final long startTime;

    public FtpClientValue(FTPClient client, long startTime) {
      this.client = client;
      this.startTime = startTime;

    }

    public FTPClient getFtpClient() {
      return client;
    }

    @Override
    public boolean needClose() {
      try {
        client.noop();
      } catch (IOException ex) {
        return true;
      }
      return System.currentTimeMillis() - startTime > timeout;
    }

    @Override
    public void afterRelease() {
//            System.err.println(client.getRemoteAddress()+" is closed!");//use for debug
    }

    @Override
    public void close() throws IOException {
      client.disconnect();
    }

  }

  private RestrictedListMap.ValueBuilder<FtpClientValue> createBuilder(FtpClientInfo info) {
    return () -> {
      FTPClient newClient = new FTPClient();
      if (info.info.getPort().isPresent()) {
        newClient.connect(info.info.getHost().get(), info.info.getPort().get());
      } else {
        newClient.connect(info.info.getHost().get());
      }
      if (info.info.getUser().isPresent()) {
        boolean loginSuccess = newClient.login(info.info.getUser().get(), info.info.getPasswd().orElse(null));
        if (!loginSuccess) {
          throw new FtpResourceException(HDSConstants.LOGIN_FAILED);
        }
      }
      return new FtpClientValue(newClient, System.currentTimeMillis());
    };
  }

  public class FtpResource {

    private final Element<FtpClientInfo, FtpClientValue> resource;

    public FtpResource(Element<FtpClientInfo, FtpClientValue> e) {
      resource = e;
    }

    public FTPClient getFtpClient() {
      return resource.getValue().getFtpClient();
    }

    public void release() {
      resource.close();
    }
  }

  public long getConnNum() {
    return resource.count();
  }

}
