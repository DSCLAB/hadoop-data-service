package umc.cdc.hds.datastorage;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.HDSUtil;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import static umc.cdc.hds.datastorage.status.filter.SortAndLimitFilter.sortAndLimit;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 * @param <M>
 * @param <F>
 */
public abstract class BaseDataStorage<M, F> implements DataStorage {

  protected abstract DataResource getDataResource(UriRequest uri) throws IOException;

  protected abstract OutputStream initinalizeOutput(DataResource res) throws IOException;

  protected abstract void removeTmpFile(DataResource res) throws IOException;

  protected abstract void removeRealFile(DataResource res) throws IOException;
  
  protected abstract void renameFile(DataResource res) throws IOException;

  protected abstract void changeRealFileTime(DataResource res, long time) throws IOException;

  protected abstract F[] internalList(DataResource res, boolean useWild) throws IOException;

  protected abstract F[] singletonArray(F file);

  protected abstract F getFile(DataResource res, UriRequest uri, boolean useWild) throws IOException;

  protected abstract Iterator<DataRecord> wrapFiles(DataResource res, F[] files) throws IOException;

  protected abstract void deleteRecursive(DataResource res, F deleteFile) throws IOException;

  protected abstract CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(
      DataResource res,
      F[] files,
      int[] accept,
      List<Exception> exs);


  protected abstract InputStream initinalizeInput(DataResource res) throws IOException;

  protected abstract DataRecord getFileRecord(DataResource res) throws IOException;

  public BaseDataStorage(Configuration conf) {

  }

  protected abstract boolean existRealFile(DataResource res) throws IOException;

  protected abstract boolean existTmpFile(DataResource res) throws IOException;

  @Override
  public DataStorageOutput create(UriRequest uri) throws IOException {
    DataResource resource = getDataResource(uri);
    try {
      return new DataStorageOutput() {
        OutputStream out = initinalizeOutput(resource);
        long writeBytes = 0l;

        @Override
        public void close() throws IOException {
          resource.close();
        }

        @Override
        public void recover() throws IOException {
          out.close();
          if (existTmpFile(resource)) {
            removeTmpFile(resource);
          }
        }

        @Override

        public void write(int b) throws IOException {
          out.write(b);
          writeBytes++;
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
          out.write(b, offset, length);
          writeBytes += length;
        }

        @Override
        public long getSize() {
          return writeBytes;
        }

        @Override
        public String getName() {
          return resource.getRealUriRequest().getName().orElse(null);
        }

        @Override
        public String getUri() {
          return resource.getRealUriRequest().toString();
        }

        @Override
        public void commit() throws IOException {
          out.close();
          if (!uri.getQuery().getQueryValueAsBoolean(HDSConstants.COMMON_URI_ARG_KEEP, true)) {
            removeTmpFile(resource);
          } else {
            if (existRealFile(resource)) {
              removeRealFile(resource);
            }
            renameFile(resource);
            Optional<Long> time = uri.getQuery().getQueryValueAsTime(HDSConstants.HDS_URI_ARG_TIME);
            if (time.isPresent()) {
              changeRealFileTime(resource, time.get());
            }
          }
        }

      };
    } catch (IOException ie) {
      resource.close();
      throw ie;
    }
  }

  @Override
  public DataStorageInput open(UriRequest uri) throws IOException {
    DataResource resource = getDataResource(uri);
    try {
      return new DataStorageInput() {
        DataRecord status = getFileRecord(resource);
        InputStream in = initinalizeInput(resource);

        @Override
        public void close() throws IOException {
          try {
            in.close();
            if (!uri.getQuery().getQueryValueAsBoolean(HDSConstants.COMMON_URI_ARG_KEEP, true)) {
              removeRealFile(resource);
            }
          } finally {
            resource.close();
          }
        }

        @Override
        public int read() throws IOException {
          return in.read();
        }

        @Override
        public int read(byte[] b, int offset, int length) throws IOException {
          return in.read(b, offset, length);
        }

        @Override
        public void recover() {
          //do nothing
        }

        @Override
        public long getSize() {
          return status.getSize();
        }

        @Override
        public String getName() {
          return status.getName();
        }

        @Override
        public String getUri() {
          return status.getUri();
        }

      };
    } catch (IOException ie) {
      resource.close();
      throw ie;
    }
  }

  @Override
  public CloseableIterator<BatchDeleteRecord> batchDelete(UriRequest uri, boolean enableWildcard) throws IOException {
    uri.setFileNameToDirName();
    try (DataResource resource = getDataResource(uri)) {
      F[] files = singletonableInternalList(resource, enableWildcard);
      IntStream.Builder faildFilesIndexBuilder = IntStream.builder();
      List<Exception> errors = new ArrayList();
      for (int i = 0; i < files.length; i++) {
        F file = files[i];
        try {
          deleteRecursive(resource, file);
        } catch (IOException ie) {
          faildFilesIndexBuilder.accept(i);
          errors.add(ie);
        }
      }
      return wrapParticalFilesWithErrorMessage(resource, files, faildFilesIndexBuilder.build().toArray(), errors);
    }
  }
  
  @Override
  public DataRecord delete(UriRequest uri, boolean isRecursive) throws IOException {
    DataRecord record;
    final boolean isDir = uri.toRealUri().endsWith("/");
    if (uri.getDir() == null) {
      uri.setDir("/");
    }
    if (uri.getPath().equals("/") 
            || (uri.getProtocol().equals(Protocol.smb) && uri.getPath().split("/").length < 3)) {
      if (isRecursive) {
        try (DataResource resource = getDataResource(uri)) {
          record = getFileRecord(resource);
        }
        uri.getQuery().put(HDSConstants.DS_URI_ARG_DIRECTORY, "true");
        uri.getQuery().put(HDSConstants.HDS_URI_ARG_NAME, "*");
        CloseableIterator<BatchDeleteRecord> rs = batchDelete(uri, true);
        if (rs.hasNext()) {
          throw new IOException("delete fails.");
        }
        return record;
      } else {
        throw new IOException("cannot delete root");
      }
    }
    Path p = new Path(uri.getPath());
    if (p.getParent() == null || p.getParent().toString().equals("/")) {
      uri.setDir("/");
    } else {
      uri.setDir(p.getParent().toString() + "/");
    }
    uri.setName(p.getName());
    try (DataResource resource = getDataResource(uri)) {
      record = getFileRecord(resource);
      if (record.getFileType().isPresent() && record.getFileType().get().equals(FileType.file)) {
        if (isDir) {
          throw new IOException("file cannot end with '/'");
        }
        removeRealFile(resource);
        return record;
      } else if (record.getFileType().isPresent() && record.getFileType().get().equals(FileType.directory)) {
        if (!isDir) {
          throw new IOException("directory must end with '/'");
        }
        resource.getRealUriRequest().getQuery().put(HDSConstants.DS_URI_ARG_DIRECTORY, "true");
        resource.getRealUriRequest().getQuery().put(HDSConstants.HDS_URI_ARG_NAME, "*");
        F[] files = singletonableInternalList(resource, true);
        if (files.length > 0 && isRecursive == false) {
          throw new IOException("directory not empty.");
        }
      } else {
        throw new IOException("no such file/directory.");
      }
    }
    Path batchPath = new Path(uri.getPath());
    uri.clearName();
    if (batchPath.getParent().toString().equals("/")) {
      uri.setDir("/");
    } else {
      uri.setDir(batchPath.getParent().toString() + "/");
    }
    uri.getQuery().put(HDSConstants.DS_URI_ARG_DIRECTORY, "true");
    uri.getQuery().put(HDSConstants.HDS_URI_ARG_NAME, batchPath.getName());
    CloseableIterator<BatchDeleteRecord> rs = batchDelete(uri, false);
    if (rs.hasNext()) {
      throw new IOException("delete fails.");
    }
    return record;
  }

  @Override
  public CloseableIterator<DataRecord> list(UriRequest uri, Query optArg) throws IOException {
    uri.setFileNameToDirName();
    try (DataResource resource = getDataResource(uri)) {
      return sortAndLimit(wrapFiles(resource, singletonableInternalList(resource, true)), optArg);
    }
  }

  private F[] singletonableInternalList(DataResource res, boolean enableWildcard) throws IOException {
    UriRequest uri = res.getRealUriRequest();
    Optional<String> fileName = uri.getQuery().getQueryValue(HDSConstants.HDS_URI_ARG_NAME);
    if (fileName.isPresent()
        && (!enableWildcard || !HDSUtil.containsWildCard(fileName.get()))) {
      uri.setName(fileName.get());
      return singletonArray(getFile(res, uri, enableWildcard));
    } else {
      return internalList(res, enableWildcard);
    }
  }

  public abstract class DataResource implements Closeable {

    private final UriRequest uri;
    private final UriRequest tmpUri;

    public DataResource(UriRequest uri) throws IOException {
      this.uri = UriParser.valueOf(uri);
      this.tmpUri = UriParser.valueOf(uri);
      tmpUri.setName(HDSUtil.getTmpPath(uri.getName().orElse("TMP")));
    }

    public UriRequest getTmpUriRequest() {
      return tmpUri;
    }

    public UriRequest getRealUriRequest() {
      return uri;
    }

    public void setRealUriRequestDir(String newDir) {
      uri.setDir(newDir);
    }

    public void setRealUriRequestName(String newFileName) {
      uri.setName(newFileName);
    }

    public abstract M getFileManager();
  }

}
