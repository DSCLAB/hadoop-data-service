package umc.cdc.hds.datastorage;

import umc.cdc.hds.datastorage.status.DataRecord;
import java.io.Closeable;
import java.io.IOException;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriRequest;

public interface DataStorage extends Closeable {

  /**
   * close the data storage service
   *
   * @throws IOException if cannot close the service
   */
  @Override
  public void close() throws IOException;

  /**
   * create file to write to data storage
   *
   * @param uri the uri form which defined from its data storage
   * @return DataStorageOutput which is extends from java.io.OutputStream , so
   * that user can use it as java.io.OutputStream and it can use more feature
   * (e.g. if write failed user can use recovry to recover file)
   * @throws IOException if create is failed. it will throw IOexception
   */
  public DataStorageOutput create(UriRequest uri) throws IOException;

  /**
   * open file to read from data storage
   *
   * @param uri the uri form which defined from its data storage
   * @return DataStorageInput which is extends from java.io.InputStream , so
   * that user can use it as java.io.InputStream and it can use more feature
   * such as file information (e.g. get file size)
   * @throws IOException if open is failed. it will throw IOexception
   */
  public DataStorageInput open(UriRequest uri) throws IOException;

  /**
   * delete files or directories(if arg directory = true) under dirctory
   *
   * @param uri the uri from which want to remove under its file
   * @param enableWildcard the name tag will use wildcard or not
   * @return the file descript for delete faild file
   * @throws IOException if occured from I/O Exception it will throw
   */
  public CloseableIterator<BatchDeleteRecord> batchDelete(UriRequest uri, boolean enableWildcard) throws IOException;

  /**
   * delete single file or directory
   *
   * @param uri the uri from which want to remove file
   * @param isRecursive set true, if you want to remove directory recursively
   * @return the file descript for delete success file
   * @throws java.io.IOException if delete file faild or it is not file it will
   * throw
   */
  public DataRecord delete(UriRequest uri, boolean isRecursive) throws IOException;

  public CloseableIterator<DataRecord> list(UriRequest uri, Query optArg) throws IOException;

  public long getConnectionNum();
}
