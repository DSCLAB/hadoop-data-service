package com.dslab.drs.exception;

import java.io.IOException;

public class DrsQueueException extends IOException {

  public DrsQueueException(String ex) {
    super(ex);
  }

  public DrsQueueException(Exception ex) {
    super(ex);
  }

}
