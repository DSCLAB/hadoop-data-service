/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromHdfs;
import umc.cdc.hds.datastorage.status.filter.HdfsDataStorageFilter;
import umc.cdc.hds.exceptions.HdfsDataStorageException;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class HdfsDataStorage extends BaseDataStorage<FileSystem, FileStatus> {

  private final FileSystem hdfs;

  public HdfsDataStorage(Configuration conf) throws IOException {
    super(conf);
    hdfs = FileSystem.get(conf);
  }

  @Override
  protected DataResource getDataResource(UriRequest uri) throws IOException {
    return new DataResource(uri) {
      @Override
      public FileSystem getFileManager() {
        return hdfs;
      }

      @Override
      public void close() throws IOException {
        //do nothing
      }

    };
  }

  @Override
  protected OutputStream initinalizeOutput(DataResource res) throws IOException {
    return res.getFileManager().create(new Path(res.getTmpUriRequest().getPath()));
  }

  @Override
  protected void removeTmpFile(DataResource res) throws IOException {
    if (!res.getFileManager().delete(new Path(res.getTmpUriRequest().getPath()), true)) {
      throw new HdfsDataStorageException(HDSConstants.REMOVE_TMP_FAILED);
    }
  }

  @Override
  protected void removeRealFile(DataResource res) throws IOException {
    if (!res.getFileManager().delete(new Path(res.getRealUriRequest().getPath()), true)) {
      throw new HdfsDataStorageException(HDSConstants.REMOVE_FILE_FAILED);
    }
  }

  @Override
  protected void renameFile(DataResource res) throws IOException {
    if (!res.getFileManager().rename(new Path(res.getTmpUriRequest().getPath()), new Path(res.getRealUriRequest().getPath()))) {
      throw new HdfsDataStorageException(HDSConstants.CHANGE_TO_FILE_FAILED);
    }
  }

  @Override
  protected void changeRealFileTime(DataResource res, long time) throws IOException {
    res.getFileManager().setTimes(new Path(res.getRealUriRequest().getPath()), time, time);
  }

  @Override
  protected FileStatus[] internalList(DataResource res, boolean useWild) throws IOException {
    return res.getFileManager().listStatus(new Path(res.getRealUriRequest().getPath()), new HdfsDataStorageFilter(res.getFileManager(),
        res.getRealUriRequest(),
        useWild));
  }

  @Override
  protected Iterator<DataRecord> wrapFiles(DataResource res, FileStatus[] files) throws IOException {
    DataRecordWrapperFromHdfs wrapper = new DataRecordWrapperFromHdfs(res.getRealUriRequest(), res.getFileManager());
    return wrapper.wrapFiles(files);
  }

  @Override
  protected void deleteRecursive(DataResource res, FileStatus deleteFile) throws IOException {
    res.getFileManager().delete(deleteFile.getPath(), true);
  }

  @Override
  protected CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(DataResource res, FileStatus[] files, int[] accept, List<Exception> exs) {
    DataRecordWrapperFromHdfs wrapper = new DataRecordWrapperFromHdfs(res.getRealUriRequest(), res.getFileManager());
    return wrapper.wrapParticalFilesWithErrorMessage(files, accept, exs);
  }

  @Override
  protected InputStream initinalizeInput(DataResource res) throws IOException {
    return res.getFileManager().open(new Path(res.getRealUriRequest().getPath()));
  }

  @Override
  protected DataRecord getFileRecord(DataResource res) throws IOException {
    DataRecordWrapperFromHdfs wrapper = new DataRecordWrapperFromHdfs(res.getRealUriRequest(), res.getFileManager());
    return wrapper.wrapSingleFile(res.getFileManager().getFileStatus(new Path(res.getRealUriRequest().getPath())));
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  protected boolean existRealFile(DataResource res) throws IOException {
    return res.getFileManager().exists(new Path(res.getRealUriRequest().getPath()));

  }

  @Override
  protected boolean existTmpFile(DataResource res) throws IOException {
    return res.getFileManager().exists(new Path(res.getTmpUriRequest().getPath()));
  }

  @Override
  protected FileStatus getFile(DataResource res, UriRequest uri, boolean useWild) throws IOException {
    try {
      FileStatus f = res.getFileManager().getFileStatus(new Path(uri.getPath()));
      HdfsDataStorageFilter filter = new HdfsDataStorageFilter(res.getFileManager(),
          res.getRealUriRequest(),
          useWild);
      return filter.accept(f.getPath()) ? f : null;
    } catch (FileNotFoundException ex) {
      return null;
    }
  }

  @Override
  protected FileStatus[] singletonArray(FileStatus file) {
    return file != null ? new FileStatus[]{file} : new FileStatus[]{};
  }

  @Override
  public long getConnectionNum() {
    return 0;
  }

}
