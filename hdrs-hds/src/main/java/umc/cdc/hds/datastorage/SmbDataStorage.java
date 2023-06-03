package umc.cdc.hds.datastorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromSmb;
import umc.cdc.hds.datastorage.status.filter.SmbDataStorageFilter;
import umc.cdc.hds.exceptions.SmbDataStorageException;
import umc.cdc.hds.mapping.AccountInfo;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class SmbDataStorage extends BaseDataStorage<SmbFile, SmbFile> {

  public SmbDataStorage(Configuration conf) {
    super(conf);
    jcifs.Config.registerSmbURLHandler();
  }

  @Override
  protected DataResource getDataResource(UriRequest uri) throws IOException {
    return new DataResource(uri) {
      @Override
      public SmbFile getFileManager() {
        return null;
      }

      @Override
      public void close() throws IOException {
        //do nothing
      }

    };
  }

  private static NtlmPasswordAuthentication getSmbAuth(UriRequest uri) throws IOException {
    if (uri.getAccountInfo().isPresent()) {
      AccountInfo acc = uri.getAccountInfo().get();
      if (acc.getHost().isPresent() && !acc.getHost().get().isEmpty()) {
        if (acc.getDomain().isPresent() || acc.getUser().isPresent() || acc.getPasswd().isPresent()) {
          return new NtlmPasswordAuthentication(acc.getDomain().orElse(null),
              acc.getUser().orElse(null),
              acc.getPasswd().orElse(null));
        } else {
          return NtlmPasswordAuthentication.ANONYMOUS;
        }
      }
    }
    throw new SmbDataStorageException(HDSConstants.NO_HOST);
  }

  @Override
  protected OutputStream initinalizeOutput(DataResource res) throws IOException {
    String parentPath = res.getTmpUriRequest().toRealUri().substring(0, res.getTmpUriRequest().toRealUri().lastIndexOf("/"));
    SmbFile parent = new SmbFile(parentPath, getSmbAuth(res.getTmpUriRequest()));
    if (!parent.exists()) {
      try {
        parent.mkdirs();
      } catch (SmbException ex) {
        throw new IOException("Create directory fails");
      }
    }
    return new SmbFile(res.getTmpUriRequest().toRealUri(), getSmbAuth(res.getTmpUriRequest())).getOutputStream();
  }

  @Override
  protected void removeTmpFile(DataResource res) throws IOException {
    new SmbFile(res.getTmpUriRequest().toRealUri(), getSmbAuth(res.getTmpUriRequest())).delete();
  }

  @Override
  protected void removeRealFile(DataResource res) throws IOException {
    new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())).delete();
  }

  @Override
  protected void renameFile(DataResource res) throws IOException {
    new SmbFile(res.getTmpUriRequest().toRealUri(), getSmbAuth(res.getTmpUriRequest())).renameTo(
        new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())));
  }

  @Override
  protected void changeRealFileTime(DataResource res, long time) throws IOException {
    new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())).setLastModified(time);
  }

  @Override
  protected SmbFile[] internalList(DataResource res, boolean useWild) throws IOException {
    return new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())).listFiles(
        new SmbDataStorageFilter(res.getRealUriRequest(), useWild));
  }

  @Override
  protected Iterator<DataRecord> wrapFiles(DataResource res, SmbFile[] files) throws IOException {
    DataRecordWrapperFromSmb wrapper = new DataRecordWrapperFromSmb(res.getRealUriRequest());
    return wrapper.wrapFiles(files);
  }

  @Override
  protected void deleteRecursive(DataResource res, SmbFile deleteFile) throws IOException {
    if (deleteFile.isDirectory() && !deleteFile.getName().endsWith("/")) {
      deleteFile = new SmbFile(res.getRealUriRequest().toRealUri() + "/", getSmbAuth(res.getRealUriRequest()));
    }
    deleteFile.delete();
  }

  @Override
  protected CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(DataResource res, SmbFile[] files, int[] accept, List<Exception> exs) {
    DataRecordWrapperFromSmb wrapper = new DataRecordWrapperFromSmb(res.getRealUriRequest());
    return wrapper.wrapParticalFilesWithErrorMessage(files, accept, exs);
  }

  @Override
  protected InputStream initinalizeInput(DataResource res) throws IOException {
    return new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())).getInputStream();
  }

  @Override
  protected DataRecord getFileRecord(DataResource res) throws IOException {
    DataRecordWrapperFromSmb wrapper = new DataRecordWrapperFromSmb(res.getRealUriRequest());
    return wrapper.wrapSingleFile(new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())));
  }

  @Override
  public void close() throws IOException {
    //do nothing
  }

  @Override
  protected boolean existRealFile(DataResource res) throws IOException {
    return new SmbFile(res.getRealUriRequest().toRealUri(), getSmbAuth(res.getRealUriRequest())).exists();
  }

  @Override
  protected boolean existTmpFile(DataResource res) throws IOException {
    return new SmbFile(res.getTmpUriRequest().toRealUri(), getSmbAuth(res.getTmpUriRequest())).exists();
  }

  @Override
  protected SmbFile getFile(DataResource res, UriRequest uri, boolean useWild) throws IOException {
    SmbFile f = new SmbFile(uri.toRealUri(), getSmbAuth(res.getRealUriRequest()));
    return f.exists() && new SmbDataStorageFilter(res.getRealUriRequest(), useWild).accept(f) ? f : null;
  }

  @Override
  protected SmbFile[] singletonArray(SmbFile file) {
    return file != null ? new SmbFile[]{file} : new SmbFile[]{};
  }

  @Override
  public long getConnectionNum() {
    return 0;
  }

}
