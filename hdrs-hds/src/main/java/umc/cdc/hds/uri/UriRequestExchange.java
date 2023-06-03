/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.uri;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.Optional;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.httpserver.Exchange;
import umc.cdc.hds.mapping.AccountInfo;

/**
 *
 * @author brandboat
 */
public class UriRequestExchange implements UriRequest {

  private final Exchange httpExchange;
  private final UriRequest uri;

  public UriRequestExchange(UriRequest uri, Exchange httpExchange)
      throws IOException {
    this.uri = uri;
    this.httpExchange = httpExchange;
  }

  public HttpExchange getHttpExchange() {
    return httpExchange;
  }

  @Override
  public Protocol getProtocol() {
    return uri.getProtocol();
  }

  @Override
  public Optional<AccountInfo> getAccountInfo() throws IOException {
    return uri.getAccountInfo();
  }

  @Override
  public String getDir() {
    return uri.getDir();
  }

  @Override
  public Optional<String> getName() {
    return uri.getName();
  }

  @Override
  public String getPath() {
    return uri.getPath();
  }

  @Override
  public Query getQuery() {
    return uri.getQuery();
  }

  @Override
  public Optional<String> getStorageId() {
    return uri.getStorageId();
  }

  @Override
  public void setProtocol(Protocol p) {
    uri.setProtocol(p);
  }

  @Override
  public void setAccountInfo(AccountInfo a) {
    uri.setAccountInfo(a);
  }

  @Override
  public void setDir(String dir) {
    uri.setDir(dir);
  }

  @Override
  public void setName(String name) {
    uri.setName(name);
  }

  @Override
  public void setQuery(Query q) {
    uri.setQuery(q);
  }

  @Override
  public String toRealUri() {
    return uri.toRealUri();
  }

  @Override
  public String toString() {
    return uri.toString();
  }

  @Override
  public void setFileNameToDirName() {
    uri.setFileNameToDirName();
  }

  @Override
  public void clearName() {
    uri.clearName();
  }
}
