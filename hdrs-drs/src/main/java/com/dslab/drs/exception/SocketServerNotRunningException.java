package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class SocketServerNotRunningException extends IOException {

  public SocketServerNotRunningException(String ex) {
    super(ex);
  }
}
