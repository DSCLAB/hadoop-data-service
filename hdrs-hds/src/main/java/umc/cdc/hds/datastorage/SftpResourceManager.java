/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import org.apache.commons.net.ftp.FTPClient;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.exceptions.SftpResourceException;
import umc.cdc.hds.mapping.AccountInfo;
import umc.cdc.hds.uri.UriRequest;
import umc.udp.core.framework.RestrictedListMap;
import umc.udp.core.framework.RestrictedListMap.Element;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.*;
import java.util.Properties;

/**
 *
 * @author jpopaholic
 */
public class SftpResourceManager implements Closeable {

  private final long timeout;
  private final RestrictedListMap<SftpClientInfo, SftpClientValue> resource;

  public SftpResourceManager(int poolSize, int checkTime, long timeout) {
    resource = new RestrictedListMap<>(poolSize, checkTime);
    this.timeout = timeout;
  }

  public SftpResource getSftpResource(UriRequest uri) throws IOException {
    SftpClientInfo info = new SftpClientInfo(uri);
    return new SftpResource(resource.get(info, createBuilder(info)));
  }

  @Override
  public void close() throws IOException {
    resource.close();
  }

  private class SftpClientInfo implements Comparable<SftpClientInfo> {

    private final AccountInfo info;

    public SftpClientInfo(UriRequest uri) throws IOException {
      this.info = uri.getAccountInfo().orElseThrow(() -> new SftpResourceException(HDSConstants.NO_HOST));
      if (!info.getHost().isPresent()) {
        throw new SftpResourceException(HDSConstants.NO_HOST);
      }
    }

    @Override
    public int compareTo(SftpClientInfo o) {
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

  public class SftpClientValue implements RestrictedListMap.Value {

    //private final FTPClient client;
    private final long startTime;
    private ChannelSftp sftp;
    private Session session;
    /** SFTP username*/
    private String username;
    /** SFTP password*/
    private String password;
    /** privateKey */
    private String privateKey;
    /** SFTP IP*/
    private String host;
    /** SFTP port*/
    private int port;

    public SftpClientValue(String username, String password, String host, int port, long startTime) {
      this.username = username;
      this.password = password;
      this.host = host;
      this.port = port;
      this.startTime = startTime;

    }

    //<---- connect sftp with privateKey, not support yet.
    public SftpClientValue(String username, String host, int port, String privateKey, long startTime) {
      this.username = username;
      this.host = host;
      this.port = port;
      this.privateKey = privateKey;
      this.startTime = startTime;

    }
    //---->

    public void login() throws SftpResourceException {
      try {
        JSch jsch = new JSch();
        if (privateKey != null) {
          jsch.addIdentity(privateKey);// set privateKey
        }
        session = jsch.getSession(username, host, port);
        if (password != null) {
          session.setPassword(password);
        }
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        session.setConfig(config);
        session.connect();
        Channel channel = session.openChannel("sftp");
        channel.connect();
        sftp = (ChannelSftp) channel;

      } catch (JSchException e) {
        throw new SftpResourceException(e.toString());
      }
    }
    public ChannelSftp getChannelSftp() {
      return sftp;
    }

    @Override
    public boolean needClose() {
      try {
        session.sendKeepAliveMsg();
        //client.noop();
      } catch (Exception e) {
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
      sftp.disconnect();
      session.disconnect();
    }
  }

  private RestrictedListMap.ValueBuilder<SftpClientValue> createBuilder(SftpClientInfo info) {
    return () -> {
      String user = "";
      String pwd = "";
      String ho = "";
      int po = 22;

      if (info.info.getPort().isPresent()) {
        ho = info.info.getHost().get();
        po = info.info.getPort().get();
      } else {
        ho = info.info.getHost().get();
      }
      if (info.info.getUser().isPresent()) {
        user = info.info.getUser().get();
        pwd = info.info.getPasswd().orElse(null);
      }
      SftpClientValue newClient = new SftpClientValue(user, pwd, ho, po ,System.currentTimeMillis());
      try{
        newClient.login();
        return newClient;
      } catch (Exception e) {
        throw new SftpResourceException(HDSConstants.LOGIN_FAILED);
      }
    };
  }

  public class SftpResource {

    private final Element<SftpClientInfo, SftpClientValue> resource;

    public SftpResource(Element<SftpClientInfo, SftpClientValue> e) {
      resource = e;
    }

    public ChannelSftp getChannelSftp() {
      return resource.getValue().getChannelSftp();
    }

    public void release() {
      resource.close();
    }
  }

  public long getConnNum() {
    return resource.count();
  }

}
