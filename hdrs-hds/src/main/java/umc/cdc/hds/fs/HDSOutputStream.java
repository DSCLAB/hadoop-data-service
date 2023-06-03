package umc.cdc.hds.fs;

import java.io.IOException;
import java.io.OutputStream;
import umc.cdc.hds.datastorage.DataStorageOutput;
import umc.cdc.hds.datastorage.HdsDataStorage;
import umc.cdc.hds.uri.UriRequest;

public class HDSOutputStream extends OutputStream {

  private final DataStorageOutput output;
  private boolean closed = false;

  @Override
  public void write(int b) throws IOException {
    output.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    output.commit();
    output.close();
    closed = true;
  }

  @Override
  public void flush() throws IOException {
  }

  public HDSOutputStream(HdsDataStorage ds, UriRequest req) throws IOException {
    output = ds.create(req);
  }
}
