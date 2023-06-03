/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.requestloadbalancer;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author jpopaholic
 */
public interface RequestLoadBalancer extends Closeable {

  /**
   * choose the best server which request can run on
   *
   * @param originRequest the RESTful api request
   * @return the plan including origin RESTful api request and the best server
   * can be run on
   * @throws IOException
   */
  public RequestRedirectPlan balanceRequest(Request originRequest) throws IOException;
}
