package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class DrsClientException extends IOException {

  public DrsClientException(String ex) {
    super(ex);
  }
}
