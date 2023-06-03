/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.mapping;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Optional;
import umc.cdc.hds.core.HDSConstants;

/**
 *
 * @author brandboat
 */
public class AccountInfo {

  private final Optional<String> id;
  private final Optional<String> domain;
  private final Optional<String> host;
  private final Optional<Integer> port;
  private final Optional<String> user;
  private final Optional<String> passwd;

  public AccountInfo(
      String id,
      String domain,
      String host,
      Integer port,
      String user,
      String passwd) {
    this.id = Optional.ofNullable(id);
    this.domain = Optional.ofNullable(domain);
    this.host = Optional.ofNullable(host);
    this.port = Optional.ofNullable(port);
    this.user = Optional.ofNullable(user);
    this.passwd = Optional.ofNullable(passwd);
  }

  public Optional<String> getId() {
    return id;
  }

  public Optional<String> getDomain() {
    return domain;
  }

  public Optional<String> getHost() {
    return host;
  }

  public Optional<Integer> getPort() {
    return port;
  }

  public Optional<String> getUser() {
    return user;
  }

  public Optional<String> getPasswd() {
    return passwd;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (id.isPresent()) {
      return sb.append("$").append(id.get()).toString();
    } else {
      return toRealString();
    }
  }

  public String toRealString() {
    StringBuilder sb = new StringBuilder();
    if (domain.isPresent()) {
      sb.append(domain.get()).append(";");
    }
    if (user.isPresent()) {
      try {
        sb.append(URLEncoder.encode(user.get(), HDSConstants.DEFAULT_CHAR_ENCODING));
      } catch (UnsupportedEncodingException ex) {
        //never happened
      }
    }
    if (passwd.isPresent()) {
      try {
        sb.append(":").append(URLEncoder.encode(passwd.get(), HDSConstants.DEFAULT_CHAR_ENCODING));
      } catch (UnsupportedEncodingException ex) {
        //never happended
      }
    }
    if (host.isPresent()) {
      if (sb.length() > 0) {
        sb.append("@");
      }
      sb.append(host.get());
    }
    if (port.isPresent()) {
      sb.append(":").append(String.valueOf(port.get()));
    }
    return sb.toString();
  }

}
