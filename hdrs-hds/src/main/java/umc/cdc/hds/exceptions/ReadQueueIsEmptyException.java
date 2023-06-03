/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.exceptions;

import java.io.EOFException;

/**
 *
 * @author jpopaholic
 */
public class ReadQueueIsEmptyException extends EOFException {

  public ReadQueueIsEmptyException() {
    super();
  }

  public ReadQueueIsEmptyException(String message) {
    super(message);
  }
}
