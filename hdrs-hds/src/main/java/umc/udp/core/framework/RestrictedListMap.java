package umc.udp.core.framework;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A restricted map is implemented by {@link ArrayList}. It invokes a thread to
 * check all {
 *
 * @like Value} for deleting idle one. It need a {@link ValueBuilder} to build {
 * @like Value} if there are enough room to add new element.
 * @param <N> {@link Name}
 * @param <V> {@link Value}
 */
public final class RestrictedListMap<
    N extends Comparable<N>, V extends RestrictedListMap.Value>
    implements Closeable {

  /**
   * Log.
   */
  private static final Log LOG = LogFactory.getLog(RestrictedListMap.class);
  /**
   * Max size.
   */
  private final int size;
  /**
   * Lock for prevent collection of list operation.
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  /**
   * List to save elements.
   */
  private final List<Element<N, V>> elements;
  /**
   * Thread pool. Only one thread will be invoked.
   */
  private final ExecutorService service = Executors.newSingleThreadExecutor();

  /**
   * Constructs a {@link RestrictedListMap} with specified size, builder and
   * check period.
   *
   * @param s Max size of elements saved in {@link RestrictedListMap}
   * @param checkTimes The period to check idle value
   */
  public RestrictedListMap(final int s, final int checkTimes) {
    size = s;
    elements = new ArrayList(s);
    service.execute(() -> {
      try {
        while (!Thread.interrupted()) {
          TimeUnit.SECONDS.sleep(checkTimes);
          lock.writeLock().lock();
          try {
            List<Element<N, V>> available = new ArrayList(s);
            elements.forEach(e -> {
              if (!e.isUsing() && e.needClose()) {
                e.close();
                try {
                  e.getValue().close();
                } catch (IOException ex) {
                  LOG.error("Element: " + e.getName() + " cannot be closed");
                }
              } else {
                available.add(e);
              }
            });
            elements.clear();
            elements.addAll(available);
          } finally {
            lock.writeLock().unlock();
          }
        }
      } catch (InterruptedException ex) {
        elements.forEach(e -> {
          e.close();
          try {
            e.getValue().close();
          } catch (IOException ex1) {
            LOG.error("Element: " + e.getName() + " cannot be closed");
          }
        });
        LOG.info(RestrictedListMap.class.getName()
            + " is closeing", ex);
      }
    });
  }

  /**
   * Gets the free element, otherwise a new elemenet will return.
   *
   * @param name Name
   * @param builder Value builder
   * @return A element
   * @throws IOException If there are no enough room to save new element
   */
  public Element<N, V> get(final N name, final ValueBuilder<V> builder)
      throws IOException {
    lock.readLock().lock();
    try {
//            LOG.info("List Size: "+elements.size());
      for (int i = 0; i != elements.size(); ++i) {
        Element<N, V> e = elements.get(i);
        if (e.getName().compareTo(name) == 0
            && e.available()) {
          return e;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    lock.writeLock().lock();
    try {
      if (elements.size() >= size) {
        throw new IOException(
            "No available resource, max size is " + size);
      }
      Element newOne = new Element(name, builder.build());
      elements.add(newOne);
      newOne.available();
      return newOne;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    service.shutdownNow();
  }

  /**
   * Builder for creating new {@link Value}.
   *
   * @param <V> Value
   */
  public interface ValueBuilder<V extends Value> {

    /**
     * @return A new value
     * @throws IOException if failed to build value
     */
    V build() throws IOException;
  }

  /**
   * Name with comparable interface. It is used for find the specified
   * {@link Value}
   */
//    public interface Name extends Comparable<Name> {
//    }
  /**
   * User defined.
   */
  public interface Value extends Closeable {

    /**
     * @return true if this value should be closed
     */
    boolean needClose();

    /**
     * Invoked after the element is closed.
     */
    void afterRelease();

    @Override
    void close() throws IOException;
  }

  /**
   * Internal object wraps the release free value to list and other operation.
   *
   * @param <N> Name
   * @param <V> Value
   */
  public static final class Element<N extends Comparable<N>, V extends Value>
      implements Closeable {

    /**
     * Using flag.
     */
    private final AtomicBoolean using = new AtomicBoolean(false);
    /**
     * Name.
     */
    private final N name;
    /**
     * Value.
     */
    private final V value;

    /**
     * Construct a element with specified {@link Name} and {@link Value}.
     *
     * @param n Name
     * @param v Value
     */
    Element(final N n, final V v) {
      name = n;
      value = v;
    }

    /**
     * @return Name
     */
    public N getName() {
      return name;
    }

    /**
     * @return Value
     */
    public V getValue() {
      return value;
    }

    /**
     * @return true if the value should be closed.
     */
    boolean needClose() {
      return value.needClose();
    }

    /**
     * @return True if the value is free now
     */
    boolean available() {
      return using.compareAndSet(false, true);
    }

    /**
     * @return True if the value is busy
     */
    boolean isUsing() {
      return using.get();
    }

    @Override
    public void close() {
      using.set(false);
      value.afterRelease();
    }
  }

  public int count() {
    return elements == null ? 0 : elements.size();
  }
}
