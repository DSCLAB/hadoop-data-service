/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.Closeable;
import java.util.Iterator;

/**
 *
 * @author brandboat
 * @param <T>
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

  @Override
  default void close() {
  }
}
