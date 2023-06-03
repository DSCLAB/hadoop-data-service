/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.exceptions;

/**
 *
 * @author jpopaholic
 */
public class LockException extends RuntimeException {

  public LockException() {
    super();
  }

  public LockException(String message) {
    super(message);
  }

  public LockException(String message, Throwable ex) {
    super(message, ex);
  }

  public LockException(Throwable ex) {
    super(ex);
  }

}
