/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.udp.core.framework;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a object to share. The really shared object is released when the
 * reference count has downed to zero. The {@link AutoCloseable#close()} will be
 * invoked if the really shared object is subclass of {@link AutoCloseable}.
 * <p>
 * For example, the class below generates singleton AtomicInteger. The
 * sharedString and sharedStringRef are different instances but they have the
 * same AtomicInteger.
 * <pre>
 * ShareableObject&lt;AtomicInteger&gt; sharedString
 *     = ShareableObject.create(null, () -> new AtomicInteger(1));
 * ShareableObject&lt;AtomicInteger&gt; sharedStringRef
 *     = ShareableObject.create((Class&lt;?&gt; clz, Object obj) -> true,
 *      () -> new AtomicInteger(1));
 * </pre>
 *
 * @param <T> The shared object's type
 */
public final class ShareableObject<T> implements AutoCloseable {
//    private static Log LOG = LogFactory.getLog(ShareableObject.class);

  /**
   * Saves the object and index. The index is incremental, so the error may be
   * happened when the index reachs the {@link Integer#MAX_VALUE}.
   */
  private static final Map<Integer, CountableObject> OBJECTS = new HashMap();
  /**
   * Synchronized the collected objects.
   */
  private static final Object GLOBAL_LOCK = new Object();
  /**
   * Indexs the object. It is used for removing the unused object in the
   * {@link HashMap}.
   */
  private static final AtomicInteger OBJECT_INDEX = new AtomicInteger(0);

  /**
   * Makes the object shareable. If no object is choosed by the {@link Agent} or
   * the {@link Agent} is null, a new object will be created by the
   * {@link Supplier}.
   *
   * @param <T> The shared object's type
   * @param agent Chooses the object to share
   * @param supplier Creates a object if necessory
   * @return A object with shareable wrap
   * @throws Exception If failed to create new object
   */
  public static <T> ShareableObject<T> create(Agent agent,
      Supplier<T> supplier) throws Exception {
    synchronized (GLOBAL_LOCK) {
      if (agent != null) {
        for (CountableObject obj : OBJECTS.values()) {
          if (agent.match(obj.get())
              && obj.incrementRef()) {
            return new ShareableObject(obj);
          }
        }
      }
      int i = OBJECT_INDEX.getAndIncrement();
      CountableObject<T> obj = new CountableObject(
          i, supplier.create());
      OBJECTS.put(i, obj);
      return new ShareableObject(obj);
    }
  }

  /**
   * Removes the unused object.
   *
   * @param beRemoved The object to remove
   */
  private static void remove(final CountableObject beRemoved) {
    synchronized (GLOBAL_LOCK) {
      OBJECTS.remove(beRemoved.getIndex());
    }
  }

  /**
   * @return The number of shared object
   */
  public static int size() {
    synchronized (GLOBAL_LOCK) {
      return OBJECTS.size();
    }
  }

  /**
   * Used to choose the reuseable and valid object.
   */
  @FunctionalInterface
  public interface Agent {

    /**
     * Finds the reuseable and valid object. The choosed object may be ignored
     * if it is closed.
     *
     * @param obj The object
     * @return True if the object is suitable
     */
    boolean match(Object obj);
  }

  /**
   * Represents a supplier of results.
   *
   * @param <T> The type of results supplied by this supplier
   */
  @FunctionalInterface
  public interface Supplier<T> {

    /**
     * Gets a result.
     *
     * @return a result
     * @throws java.lang.Exception If failed to get a result
     */
    T create() throws Exception;
  }
  /**
   * The object to share.
   */
  private final CountableObject<T> obj;
  /**
   * Indicates the {@link ShareableObject#close()} is invoked or not.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private ShareableObject(final CountableObject<T> object) {
    obj = object;
  }

  public boolean hasReference() {
    return obj.getReferenceCount() != 0;
  }

  /**
   * @return Current reference count
   */
  public int getReferenceCount() {
    return obj.getReferenceCount();
  }

  /**
   * Checks the internal count.
   *
   * @return True if the internal object is closed
   */
  public boolean isClosed() {
    return isClosed.get();
  }

  /**
   * @return The object to share
   */
  public T get() {
    if (isClosed()) {
      throw new RuntimeException("This outer object is closed");
    }
    return obj.get();
  }

  @Override
  public void close() throws Exception {
    if (isClosed.compareAndSet(false, true)) {
      obj.close();
    }
  }

  private static class CountableObject<T> implements AutoCloseable {

    private final int index;
    private final T object;
    private final AtomicInteger refCount = new AtomicInteger(1);

    CountableObject(final int uuid, final T obj) {
      index = uuid;
      object = obj;
    }

    int getIndex() {
      return index;
    }

    T get() {
      if (isClosed()) {
        throw new RuntimeException("The inner object is closed");
      }
      return object;
    }

    boolean isClosed() {
      return refCount.get() <= 0;
    }

    int getReferenceCount() {
      return refCount.get();
    }

    boolean incrementRef() {
      return refCount.updateAndGet(x -> x <= 0 ? 0 : x + 1) != 0;
    }

    @Override
    public void close() throws Exception {
      if (refCount.decrementAndGet() == 0
          && object instanceof AutoCloseable) {
        ((AutoCloseable) object).close();
        remove(this);
      }
    }
  }
}
