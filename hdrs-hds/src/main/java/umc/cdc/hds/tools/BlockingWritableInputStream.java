/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.exceptions.ReadQueueIsEmptyException;

/**
 *
 * @author jpopaholic
 */
public class BlockingWritableInputStream extends InputStream implements Appendable {

  private final BlockingQueue<Entry<byte[], Integer>> queue;
  private byte[] currentBytes;
  private int currentReadBytePos;
  private int currentEndBytePos;
  private final int defaultByteSize;
  private final PreparedByteArray preparedBytes;
  private boolean finalData;

  public BlockingWritableInputStream(int defaultByteSize, int queueSize) {
    queue = new ArrayBlockingQueue<>(queueSize);
    currentBytes = null;
    currentReadBytePos = 0;
    currentEndBytePos = 0;
    this.defaultByteSize = defaultByteSize;
    preparedBytes = new PreparedByteArray();
    finalData = false;
  }

  public void setNoDataComing() throws InterruptedException {
    finalData = true;
    preparedBytes.doFinalFlush();
  }

  private boolean currentDataEnd() {
    return currentReadBytePos == currentEndBytePos;
  }

  private Entry<byte[], Integer> getNewDataFromQueue() throws ReadQueueIsEmptyException, InterruptedException {
    if (queue.isEmpty()) {
      throw new ReadQueueIsEmptyException("Need to write something into queue");
    }
    return queue.take();
  }

  private void setCurrentData(Entry<byte[], Integer> newData) {
    currentBytes = newData.getKey();
    currentEndBytePos = newData.getValue();
    currentReadBytePos = 0;
  }

  @Override
  public int read() throws IOException {
    if (currentDataEnd()) {
      try {
        setCurrentData(getNewDataFromQueue());
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      } catch (ReadQueueIsEmptyException eof) {
        if (finalData) {
          return -1;
        } else {
          throw eof;
        }
      }
    }
    int result = currentBytes[currentReadBytePos];
    currentReadBytePos++;
    return result;
  }

  @Override
  public int read(byte[] b, int offset, int length) throws ReadQueueIsEmptyException, IOException {
    if (currentDataEnd()) {
      try {
        setCurrentData(getNewDataFromQueue());
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      } catch (ReadQueueIsEmptyException eof) {
        if (finalData) {
          return -1;
        } else {
          throw eof;
        }
      }
    }
    if (currentReadBytePos + length < currentEndBytePos) {
      System.arraycopy(currentBytes, currentReadBytePos, b, offset, length);
      currentReadBytePos = currentReadBytePos + length;
      return length;
    } else {
      System.arraycopy(currentBytes, currentReadBytePos, b, offset, currentEndBytePos - currentReadBytePos);
      int result = currentEndBytePos - currentReadBytePos;
      currentReadBytePos = currentEndBytePos;
      return result;
    }
  }

  @Override
  public Appendable append(CharSequence csq) throws IOException {
    if (finalData) {
      throw new IOException("data is end,cannot put any new data into it");
    }
    byte[] data = Bytes.toBytes(csq.toString());
    if (data.length == 0) {
      return this;
    }
    try {
      preparedBytes.putDataIntoPreparedByteArray(data);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
    return this;
  }

  @Override
  public Appendable append(CharSequence csq, int start, int end) throws IOException {
    if (finalData) {
      throw new IOException("data is end,cannot put any new data into it");
    }
    byte[] data = Bytes.toBytes(csq.subSequence(start, end).toString());
    if (data.length == 0) {
      return this;
    }
    try {
      preparedBytes.putDataIntoPreparedByteArray(data);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
    return this;
  }

  @Override
  public Appendable append(char c) throws IOException {
    if (finalData) {
      throw new IOException("data is end,cannot put any new data into it");
    }
    byte[] data = new byte[1];
    data[0] = (byte) c;
    try {
      preparedBytes.putDataIntoPreparedByteArray(data);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
    return this;
  }

  @Override
  public void close() throws IOException {
    queue.clear();
    currentBytes = null;
  }

  private class PreparedByteArray {

    private byte[] preparedByte;
    private int preparedEndBytePos;

    public PreparedByteArray() {
      preparedByte = null;
      preparedEndBytePos = 0;
    }

    private void initinalPreparedByteArray(int size) {
      preparedByte = new byte[size];
      preparedEndBytePos = 0;
    }

    private void doPutDataIntoPreparedByteArray(byte[] data) {
      System.arraycopy(data, 0, preparedByte, preparedEndBytePos, data.length);
      preparedEndBytePos += data.length;
    }

    public void putDataIntoPreparedByteArray(byte[] data) throws InterruptedException {
      while (true) {
        if (preparedByte != null && preparedEndBytePos + data.length <= preparedByte.length) {
          doPutDataIntoPreparedByteArray(data);
          return;
        } else {
          if (preparedByte != null) {
            flushPreparedByte();
          }
          int size = data.length <= defaultByteSize ? defaultByteSize : data.length;
          initinalPreparedByteArray(size);
        }
      }
    }

    private void flushPreparedByte() throws InterruptedException {
      queue.put(new SimpleEntry<>(preparedByte, preparedEndBytePos));
    }

    public void doFinalFlush() throws InterruptedException {
      if (preparedByte != null) {
        queue.put(new SimpleEntry<>(preparedByte, preparedEndBytePos));
      }
    }
  }
}
