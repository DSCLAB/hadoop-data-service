package umc.cdc.hds.datastorage;

import java.io.IOException;
import java.io.InputStream;

public abstract class DataStorageInput extends InputStream implements DataInfo {

  /**
   * close the InputStream
   *
   * @throws IOException it it cannot close,it will throw
   */
  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract int read() throws IOException;

  @Override
  public abstract int read(byte[] b, int offset, int length) throws IOException;

  /**
   * User may want to delete origin file after moving it into hds or elsewhere,
   * but when error occurs we have to recover everything, this method avoid
   * deleting origin file when error occurs.
   */
  public abstract void recover();
}
