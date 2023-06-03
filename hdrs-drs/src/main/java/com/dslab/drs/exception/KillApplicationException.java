package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class KillApplicationException extends IOException {

  public KillApplicationException(String ex) {
    super(ex);
  }
}
