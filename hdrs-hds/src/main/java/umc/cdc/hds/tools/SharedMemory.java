/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author brandboat
 */
public class SharedMemory implements Closeable {

  private AtomicBoolean isClosed;
  private final BlockingQueue<ByteBuffer> bufferPool;
  private final long timeout;
  private final long queueSize;

  public SharedMemory(int queueSize, int bufferSize, long timeout) {
    this.bufferPool = new ArrayBlockingQueue(queueSize);
    this.queueSize = queueSize;
    isClosed = new AtomicBoolean(false);
    this.timeout = timeout;
    for (int i = 0; i < queueSize; i++) {
      bufferPool.add(ByteBuffer.allocate(bufferSize));
    }
  }

  public ByteBuffer take() throws IOException {
    if (isClosed.get()) {
      throw new RuntimeException("SharedMemory is closed");
    }
    ByteBuffer buf;
    try {
      buf = bufferPool.poll(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    if (buf != null) {
      return buf;
    }
    throw new IOException("HDS write buffer is full, please wait for "
        + "other task done. Timeout for waiting buffer: " + timeout + " ms");
  }

  public void restore(ByteBuffer buf) {
    buf.clear();
    bufferPool.add(buf);
  }

  public long getQueueSize() {
    return queueSize;
  }

  public long getTakenNum() {
    return bufferPool == null ? 0 : bufferPool.remainingCapacity();
  }

  @Override
  public void close() throws IOException {
    isClosed.set(true);
    bufferPool.clear();
  }
}
