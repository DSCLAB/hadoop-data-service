package com.dslab.drs.hdsclient;

import java.util.List;

/**
 *
 * @author caca
 */
public class LoadingContent {

  private String host;
  private String port;
  private List<LoadingOperation> operation;

  public String getHost() {
    return this.host;
  }

  public String getPort() {
    return this.port;
  }

  public List<LoadingOperation> getLoading() {
    return this.operation;
  }

}
