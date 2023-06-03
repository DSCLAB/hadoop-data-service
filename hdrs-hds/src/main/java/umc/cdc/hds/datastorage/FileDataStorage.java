/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromFile;
import umc.cdc.hds.datastorage.status.filter.FileDataStorageFilter;
import umc.cdc.hds.exceptions.FileDataStorageException;
import umc.cdc.hds.httpserver.HdsHttpServer;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class FileDataStorage extends BaseDataStorage<File, File> {

  private static final Log LOG = LogFactory.getLog(HdsHttpServer.class);
  public FileDataStorage(Configuration conf) {
    super(conf);
  }

  @Override
  protected DataResource getDataResource(UriRequest uri) throws IOException {
    return new DataResource(uri) {
      @Override
      public File getFileManager() {
        return null;
      }

      @Override
      public void close() throws IOException {

      }

    };
  }

  @Override
  protected OutputStream initinalizeOutput(DataResource res) throws IOException {
    initMkdirs(res);
    return new FileOutputStream(res.getTmpUriRequest().getPath());
  }

  @Override
  protected void removeTmpFile(DataResource res) throws IOException {
    if (!new File(res.getTmpUriRequest().getPath()).delete()) {
      throw new FileDataStorageException(HDSConstants.REMOVE_TMP_FAILED);
    }
  }

  @Override
  protected void removeRealFile(DataResource res) throws IOException {
    if (!new File(res.getRealUriRequest().getPath()).delete()) {
      throw new FileDataStorageException(HDSConstants.REMOVE_FILE_FAILED);
    }
  }

  @Override
  protected void renameFile(DataResource res) throws IOException {
    if (!new File(res.getTmpUriRequest().getPath()).renameTo(new File(res.getRealUriRequest().getPath()))) {
      throw new FileDataStorageException(HDSConstants.CHANGE_TO_FILE_FAILED);
    }
  }

  @Override
  protected void changeRealFileTime(DataResource res, long time) throws IOException {
    if (!new File(res.getRealUriRequest().getPath()).setLastModified(time)) {
      throw new FileDataStorageException(HDSConstants.SET_FILE_TIME_FAILED);
    }
  }

  @Override
  protected File[] internalList(DataResource res, boolean useWild) throws IOException {
    return new File(res.getRealUriRequest().getPath()).listFiles(
        new FileDataStorageFilter(res.getRealUriRequest(), useWild));
  }

  @Override
  protected Iterator<DataRecord> wrapFiles(DataResource res, File[] files) throws IOException {
    DataRecordWrapperFromFile wrapper = new DataRecordWrapperFromFile(res.getRealUriRequest());
    return wrapper.wrapFiles(files);
  }

  @Override
  protected void deleteRecursive(DataResource res, File deleteFile) throws IOException {
    if (deleteFile.isDirectory()) {
      FileUtils.deleteDirectory(deleteFile);
    } else if (!deleteFile.delete()) {
      throw new FileDataStorageException(HDSConstants.REMOVE_FILE_FAILED);
    }
  }

  @Override
  protected CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(DataResource res, File[] files, int[] accept, List<Exception> exs) {
    DataRecordWrapperFromFile wrapper = new DataRecordWrapperFromFile(res.getRealUriRequest());
    return wrapper.wrapParticalFilesWithErrorMessage(files, accept, exs);
  }

  @Override
  protected InputStream initinalizeInput(DataResource res) throws IOException {
    return new FileInputStream(res.getRealUriRequest().getPath());
  }

  @Override
  protected DataRecord getFileRecord(DataResource res) throws IOException {
    DataRecordWrapperFromFile wrapper = new DataRecordWrapperFromFile(res.getRealUriRequest());
    return wrapper.wrapSingleFile(new File(res.getRealUriRequest().getPath()));
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  protected boolean existRealFile(DataResource res) throws IOException {
    try {
      return new File(res.getRealUriRequest().getPath()).exists();
    } catch (SecurityException se) {
      throw new FileDataStorageException(se);
    }
  }

  @Override
  protected boolean existTmpFile(DataResource res) throws IOException {
    try {
      return new File(res.getTmpUriRequest().getPath()).exists();
    } catch (SecurityException se) {
      throw new FileDataStorageException(se);
    }
  }

  @Override
  protected File getFile(DataResource res, UriRequest uri, boolean useWild) throws IOException {
    File f = new File(uri.getPath());
    return f.exists() && new FileDataStorageFilter(res.getRealUriRequest(), useWild).accept(f) ? f : null;
  }

  @Override
  protected File[] singletonArray(File file) {
    return file != null ? new File[]{file} : new File[]{};
  }

  @Override
  public long getConnectionNum() {
    return 0;
  }

  /*@Editor yoyo*/
  private void initMkdirs(DataResource res) throws IOException{
    File targetFile = new File(res.getTmpUriRequest().getPath());
    File targetFileParent = new File(targetFile.getParent());
    while (!(targetFileParent.toString().matches("/"))) {
      if (targetFileParent.isFile())
          throw new IOException("Your target path is conflicted by the same name file in the part of your path.");
      targetFileParent = targetFileParent.getParentFile();
    }
    targetFile.getParentFile().mkdirs();
  }

}
