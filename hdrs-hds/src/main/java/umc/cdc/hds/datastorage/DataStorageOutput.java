package umc.cdc.hds.datastorage;

import java.io.IOException;
import java.io.OutputStream;

public abstract class DataStorageOutput extends OutputStream implements DataInfo {

  /**
   * close the OutputStream
   *
   * @throws IOException it it cannot close,it will throw
   */
  @Override
  public abstract void close() throws IOException;

  /**
   * if write failed user can use this method to delete failed file
   *
   * @throws IOException if it delete failed , it will throw
   */
  public abstract void recover() throws IOException;

  public abstract void commit() throws IOException;

  @Override
  public abstract void write(int b) throws IOException;

  @Override
  public abstract void write(byte[] b, int offset, int length) throws IOException;

}
