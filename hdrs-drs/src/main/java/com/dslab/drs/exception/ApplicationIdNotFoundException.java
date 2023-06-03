package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class ApplicationIdNotFoundException extends IOException {

  public ApplicationIdNotFoundException(String ex) {
    super(ex);
  }
}
