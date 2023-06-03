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
public class JdbcDataStorageException extends IOException {

  public JdbcDataStorageException() {
    super();
  }

  public JdbcDataStorageException(String message) {
    super(message);
  }

  public JdbcDataStorageException(Exception otherException) {
    super(otherException);
  }

  public JdbcDataStorageException(String message, Exception cause) {
    super(message, cause);
  }

}
