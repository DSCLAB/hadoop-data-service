package umc.udp.core.framework;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Atomic-clseable class. It guarantee the close method is invoked only once.
 */
public abstract class AtomicCloseable implements Closeable {

  /**
   * Close flag.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * @return True if is closed
   */
  public final boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Internal close to inherit.
   *
   * @throws IOException If any error happen
   */
  protected abstract void internalClose() throws IOException;

  @Override
  public final void close() throws IOException {
    if (isClosed.compareAndSet(false, true)) {
      internalClose();
    }
  }
}
