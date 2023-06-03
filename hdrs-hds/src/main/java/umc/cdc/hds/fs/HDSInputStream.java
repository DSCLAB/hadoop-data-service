package umc.cdc.hds.fs;

import java.io.IOException;
import org.apache.hadoop.fs.FSInputStream;
import umc.cdc.hds.datastorage.DataStorageInput;
import umc.cdc.hds.datastorage.HdsDataStorage;
import umc.cdc.hds.datastorage.HdsDataStorage.HdsDataInput;
import umc.cdc.hds.uri.UriRequest;

public class HDSInputStream extends FSInputStream {

  private boolean closed = false;
  HdsDataInput hdsInput = null;

  @Override
  public synchronized long getPos() throws IOException {
    return hdsInput.getPos();
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    hdsInput.seek(pos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    return hdsInput.read();
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    return hdsInput.read(buf, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    IOException exception = null;
    try {
      super.close();
    } catch (IOException ex) {
      exception = ex;
    }
    try {
      hdsInput.close();
    } catch (IOException ex) {
      exception = ex;
    }
    if (exception != null) {
      throw exception;
    }
    closed = true;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  public HDSInputStream(HdsDataStorage ds, UriRequest req) throws IOException {
    DataStorageInput input = ds.open(req);
    if (input instanceof HdsDataInput) {
      hdsInput = (HdsDataInput) input;
    } else {
      throw new IOException("inputstream is not hds stream.");
    }
  }
}
