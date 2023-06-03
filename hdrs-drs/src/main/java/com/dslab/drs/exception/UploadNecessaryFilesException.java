package com.dslab.drs.exception;

import java.io.IOException;

/**
 *
 * @author chen10
 */
public class UploadNecessaryFilesException extends IOException {

  public UploadNecessaryFilesException(String ex) {
    super(ex);
  }

  public UploadNecessaryFilesException(Exception ex) {
    super(ex);
  }

}
