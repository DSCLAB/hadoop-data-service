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
public class RequestRedirectPlan {

  private ServerName bestRedirectServer;

  public RequestRedirectPlan(ServerName bestRedirectServer) {
    this.bestRedirectServer = bestRedirectServer;
  }

  public ServerName getBestRedirectServer() {
    return bestRedirectServer;
  }

  public void setRedirectServer(ServerName server) {
    bestRedirectServer = server;
  }
}
