/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import java.io.IOException;
import java.net.URL;

/**
 *
 * @author brandboat
 */
public class RedirectionException extends IOException {

  private final URL redirectUrl;
  private final String method;

  public RedirectionException(String errorMessage, URL url, String m) {
    super(errorMessage);
    redirectUrl = url;
    method = m;
  }

  public URL getRedirectUrl() {
    return redirectUrl;
  }

  public String getMethod() {
    return method;
  }
}
