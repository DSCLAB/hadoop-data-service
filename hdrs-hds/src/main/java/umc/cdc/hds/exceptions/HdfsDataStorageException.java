/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.exceptions;

import java.io.IOException;

/**
 *
 * @author jpopaholic
 */
public class HdfsDataStorageException extends IOException {

  public HdfsDataStorageException() {
    super();
  }

  public HdfsDataStorageException(String message) {
    super(message);
  }
}
