/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.mapping;

/**
 *
 * @author brandboat
 */
public class AccountInfoBuilder {

  private String id = null;
  private String domain = null;
  private String host = null;
  private Integer port = null;
  private String user = null;
  private String passwd = null;

  public AccountInfoBuilder() {
  }

  public AccountInfoBuilder setId(final String id) {
    this.id = id;
    return this;
  }

  public AccountInfoBuilder setDomain(final String domain) {
    this.domain = domain;
    return this;
  }

  public AccountInfoBuilder setHost(final String host) {
    this.host = host;
    return this;
  }

  public AccountInfoBuilder setPort(final Integer port) {
    this.port = port;
    return this;
  }

  public AccountInfoBuilder setUser(final String user) {
    this.user = user;
    return this;
  }

  public AccountInfoBuilder setPasswd(final String passwd) {
    this.passwd = passwd;
    return this;
  }

  public AccountInfo build() {
    return new AccountInfo(id, domain, host, port, user, passwd);
  }
}
