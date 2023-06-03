/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.requestloadbalancer;

import org.apache.hadoop.hbase.ServerName;

/**
 *
 * @author jpopaholic
 */
public class Request {

  private final ServerName serverName;
  private final Action action;

  public Request(ServerName server, Action action) {
    serverName = server;
    this.action = action;
  }

  public ServerName getServerName() {
    return serverName;
  }

  public Action getAction() {
    return action;
  }

  public static enum Action {
    ACCESS,
    DELETE,
    BATCHDELETE,
    MAPPING,
    LIST,
    LOADING,
    KILL
  }
}
