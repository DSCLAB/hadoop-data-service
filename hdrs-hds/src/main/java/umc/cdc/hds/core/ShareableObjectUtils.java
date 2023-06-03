/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.requestloadbalancer.RequestLoadBalancer;
import umc.cdc.hds.requestloadbalancer.HDSRequestLoadBalancer;
import umc.udp.core.framework.ShareableObject;

/**
 * An utility that create SharableObject.
 *
 * @author brandboat
 */
public final class ShareableObjectUtils {

  private ShareableObjectUtils() {
  }

  public static ShareableObject<RequestLoadBalancer> createRequestLoadBalancer(
      Configuration conf) throws Exception {
    return ShareableObject.<RequestLoadBalancer>create(
        (o) -> o instanceof RequestLoadBalancer ? true : false,
        () -> new HDSRequestLoadBalancer(conf));
  }
}
