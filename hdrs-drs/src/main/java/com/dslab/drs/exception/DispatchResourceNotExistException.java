package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class DispatchResourceNotExistException extends IOException {

  public DispatchResourceNotExistException(String ex) {
    super(ex);
  }
}
