package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class ConfigEmptyException extends IOException {

  public ConfigEmptyException(String ex) {
    super(ex);
  }
}
