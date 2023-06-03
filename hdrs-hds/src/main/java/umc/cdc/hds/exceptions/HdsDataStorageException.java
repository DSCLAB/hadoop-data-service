/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.exceptions;

import java.io.IOException;

/**
 *
 * @author brandboat
 */
public class HdsDataStorageException extends IOException {

  public HdsDataStorageException() {
    super();
  }

  public HdsDataStorageException(String message) {
    super(message);
  }

  public HdsDataStorageException(IOException ex) {
    super(ex);
  }
}
