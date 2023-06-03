package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class SocketServerException extends IOException {

  public SocketServerException(String ex) {
    super(ex);
  }
}
