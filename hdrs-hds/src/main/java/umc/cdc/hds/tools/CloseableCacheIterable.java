/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 *
 * @author brandboat
 * @param <E>
 */
public abstract class CloseableCacheIterable<E>
    implements Iterable<E>, Closeable {

  private Queue<E> cache = new LinkedList<>();
  private E next;

  public abstract Queue<E> setCache() throws IOException;

  @Override
  public abstract void close();

  @Override
  public CloseableIterator<E> iterator() {
    return new CloseableIterator<E>() {

      @Override
      public void close() {
        CloseableCacheIterable.this.close();
      }

      @Override
      public boolean hasNext() {
        if (next == null) {
          try {
            next = nextWithCache();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      @Override
      public E next() {
        if (hasNext()) {
          return next;
        }
        return null;
      }

    };
  }

  private E nextWithCache() throws IOException {

    if (cache.isEmpty()) {
      loadCache();
    }

    if (cache != null && cache.size() > 0) {
      return cache.poll();
    }

    return null;
  }

  private void loadCache() throws IOException {
    cache = setCache();
  }
}
