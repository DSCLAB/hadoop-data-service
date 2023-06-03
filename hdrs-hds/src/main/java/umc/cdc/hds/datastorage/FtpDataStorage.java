package umc.cdc.hds.datastorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.FtpResourceManager.FtpResource;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.DataRecordBuilder;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromFtp;
import umc.cdc.hds.datastorage.status.filter.FtpDataStorageFilter;
import umc.cdc.hds.exceptions.FtpDataStorageException;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class FtpDataStorage extends BaseDataStorage<FTPClient, FTPFile> {

  private FtpResourceManager manager;
  private static final Log LOG = LogFactory.getLog(FtpDataStorage.class);

  public FtpDataStorage(Configuration conf) {
    this(conf, conf.getInt(HDSConstants.FTP_POOL_SIZE, HDSConstants.DEFAULT_FTP_POOL_SIZE),
        conf.getInt(HDSConstants.FTP_POOL_CHECKTIME, HDSConstants.DEFAULT_FTP_POOL_CHECKTIME),
        conf.getLong(HDSConstants.FTP_CONNECTION_TIMEOUT, HDSConstants.DEFAULT_FTP_CONNECTION_TIMEOUT));
  }

  /**
   * get FtpDataStorge with connection pool, parameter is connection pool
   * setting
   *
   * @param poolSize the connection pool size
   * @param checkTime how long will pool to check whether the connection is
   * timeout
   * @param timeout connection timeout
   * @return FtpDataStorage including connection pool
   */
  private FtpDataStorage(Configuration conf, int poolSize, int checkTime, long timeout) {
    super(conf);
    manager = new FtpResourceManager(poolSize, checkTime, timeout);
  }

  @Override
  protected DataResource getDataResource(UriRequest uri) throws IOException {
    FtpResource resource = manager.getFtpResource(uri);
    return new DataResource(uri) {
      @Override
      public FTPClient getFileManager() {
        return resource.getFtpClient();
      }

      @Override
      public void close() throws IOException {
        resource.release();
      }
    };
  }
  
  private void createDirectoryIfNotExists(FTPClient client, String path) throws IOException {
    String tmp = client.printWorkingDirectory();
    boolean dirExists = true;
    client.changeWorkingDirectory("/");
    for (String dir : path.split("/")) {
      if (!dir.isEmpty()) {
        if (dirExists) {
          dirExists = client.changeWorkingDirectory(dir);
        }
        if (!dirExists) {
          if (!client.makeDirectory(dir)) {
            client.changeWorkingDirectory(tmp);
            throw new IOException("Create directory fails.");
          }
          if (!client.changeWorkingDirectory(dir)) {
            client.changeWorkingDirectory(tmp);
            throw new IOException("Create directory fails.");
          }
        }
      }
    }
    client.changeWorkingDirectory(tmp);
  }

  @Override
  protected OutputStream initinalizeOutput(DataResource res) throws IOException {
    FTPClient client = res.getFileManager();
    if (res.getRealUriRequest().getQuery().getQueryValueAsTime(HDSConstants.HDS_URI_ARG_TIME).isPresent()
        && !client.hasFeature("MFMT")) {
      throw new FtpDataStorageException("FTP Server doesn't support MFMT command!");
    }
    createDirectoryIfNotExists(client, res.getTmpUriRequest().getDir());
    try {
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.enterLocalPassiveMode();
      OutputStream fout = client.storeFileStream(res.getTmpUriRequest().getPath());
      if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
        if (fout != null) {
          fout.close();
        }
        throw new FtpDataStorageException(client.getReplyString());
      }
      return new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          fout.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          fout.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
          fout.close();
          if (!client.completePendingCommand()) {
            throw new FtpDataStorageException(client.getReplyString());
          }
        }

      };
    } catch (IOException ie) {
      throw new FtpDataStorageException(client.getReplyString());
    }
  }

  @Override
  protected void removeTmpFile(DataResource res) throws IOException {
    FTPClient client = res.getFileManager();
    if (!client.deleteFile(res.getTmpUriRequest().getPath())) {
      throw new FtpDataStorageException(client.getReplyString());
    }
  }

  @Override
  protected void removeRealFile(DataResource res) throws IOException {
    FTPClient client = res.getFileManager();
    if (!client.deleteFile(res.getRealUriRequest().getPath())) {
      throw new FtpDataStorageException(client.getReplyString());
    }
  }

  @Override
  protected void renameFile(DataResource res) throws IOException {
    FTPClient client = res.getFileManager();
    if (!client.rename(res.getTmpUriRequest().getPath(), res.getRealUriRequest().getPath())) {
      throw new FtpDataStorageException(HDSConstants.CHANGE_TO_FILE_FAILED);
    }
  }

  @Override
  protected void changeRealFileTime(DataResource res, long time) throws IOException {
    FTPClient client = res.getFileManager();
    if (!client.setModificationTime(res.getRealUriRequest().getPath(), toFtpTimeFormat(time))) {
      throw new FtpDataStorageException(client.getReplyString());
    }
  }

  private static String toFtpTimeFormat(long time) {
    DateTime dt = new DateTime(time);
    DateTimeFormatter fmt = DateTimeFormat.forPattern(HDSConstants.FTP_TIME_FORMAT)
        .withZone(DateTimeZone.UTC);
    return dt.toString(fmt);
  }

  @Override
  protected FTPFile[] internalList(DataResource res, boolean useWild) throws IOException {
    FTPClient client = res.getFileManager();
    UriRequest uri = res.getRealUriRequest();
    client.enterLocalPassiveMode();
    FTPFile[] files;
    if (client.hasFeature("MLSD")) {
      files = client.mlistDir(uri.getPath(),
          new FtpDataStorageFilter(client.getRemoteAddress().getHostName(), uri, useWild));
    } else {
      LOG.warn("FTP Server doesn't have MLSD , so use LIST instead.");
      files = client.listFiles(uri.getPath(),
          new FtpDataStorageFilter(client.getRemoteAddress().getHostName(), uri, useWild));
    }
    return files;
  }


  @Override
  protected Iterator<DataRecord> wrapFiles(DataResource res, FTPFile[] files) throws IOException {
    DataRecordWrapperFromFtp wrapper = new DataRecordWrapperFromFtp(res.getRealUriRequest(),
        res.getFileManager().getRemoteAddress().getHostName());
    return wrapper.wrapFiles(files);
  }


  @Override
  protected void deleteRecursive(DataResource res, FTPFile deleteFile) throws IOException {
    if (deleteFile.isDirectory()) {
      boolean isSuccess;
      if (deleteFile.getName().startsWith("/")) {
        isSuccess = deleteDirectory(res.getFileManager(), deleteFile.getName());
      } else {
        isSuccess = deleteDirectory(res.getFileManager(), res.getRealUriRequest().getDir() + deleteFile.getName());
      }
      if (!isSuccess) throw new IOException("delete fails");
    } else {
      StringBuilder builder = new StringBuilder();
      builder.append(res.getRealUriRequest().getDir()).append(deleteFile.getName());
      if (!res.getFileManager().deleteFile(builder.toString())) {
        throw new FtpDataStorageException(res.getFileManager().getReplyString());
      }
    }
  }

  private boolean deleteDirectory(FTPClient client, String parent) throws IOException {
    FTPFile[] childerns = client.listFiles(parent);
    for (FTPFile children : childerns) {
      if (children.isDirectory()) {
        deleteDirectory(client, parent + "/" + children.getName());
      } else {
        client.deleteFile(parent + "/" + children.getName());
      }
    }
    return client.removeDirectory(parent);
  }

  @Override
  protected CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(
      DataResource res,
      FTPFile[] files,
      int[] accept,
      List<Exception> exs) {
    DataRecordWrapperFromFtp wrapper = new DataRecordWrapperFromFtp(
        res.getRealUriRequest(),
        res.getFileManager().getRemoteAddress().getHostName());
    return wrapper.wrapParticalFilesWithErrorMessage(files, accept, exs);
  }

  @Override
  protected InputStream initinalizeInput(DataResource res) throws IOException {
    FTPClient client = res.getFileManager();
    client.enterLocalPassiveMode();
    client.setFileType(FTP.BINARY_FILE_TYPE);
    InputStream fin = client.retrieveFileStream(res.getRealUriRequest().getPath());
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      if (fin != null) {
        fin.close();
      }
      throw new FtpDataStorageException(client.getReplyString());
    }
    return new InputStream() {
      @Override
      public int read() throws IOException {
        return fin.read();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return fin.read(b, off, len);
      }

      @Override
      public void close() throws IOException {
        fin.close();
        if (!client.completePendingCommand()) {
          throw new FtpDataStorageException(client.getReplyString());
        }
      }

    };
  }

  @Override
  protected DataRecord getFileRecord(DataResource res)
      throws IOException {
    FTPClient client = res.getFileManager();
    client.enterLocalPassiveMode();
    FTPFile file = null;
    DataRecordWrapperFromFtp wrapper = new DataRecordWrapperFromFtp(
        res.getRealUriRequest(),
        client.getRemoteAddress().getHostName());
    if (client.hasFeature("MLST")) {
      file = client.mlistFile(res.getRealUriRequest().getPath());
    } else {
      LOG.warn("FTP Server doesn't have MLST , so use LIST instead.");
      Path p = new Path(res.getRealUriRequest().getPath());
      if (p.getParent() == null) { // path is root
        Map<String, Double> dataOwner = new TreeMap<>();
        dataOwner.put(client.getLocalAddress().getHostAddress(), 1.0);
        return new DataRecordBuilder()
                .setUri(res.getRealUriRequest().toRealUri())
                .setProtocol(Protocol.ftp)
                .setName("")
                .setSize(0)
                .setUploadTime(0)
                .setFileType(DataRecord.FileType.directory)
                .setOwner(dataOwner)
                .build();
      }
      FTPFile[] ftpFiles = client.listFiles(p.getParent().toString());
      if (ftpFiles != null) {
        for (FTPFile ftpFile : ftpFiles) {
          if (ftpFile.getName().equals(p.getName())) { // file found in dir
            file = ftpFile;
            break;
          }
        }
      }
    }
    if (file == null) {
      throw new FtpDataStorageException(HDSConstants.NO_FILE);
    }
    return wrapper.wrapSingleFile(file);
  }

  @Override
  public void close() throws IOException {
    manager.close();
  }

  @Override
  protected boolean existRealFile(DataResource res) throws IOException {
    return res.getFileManager().listFiles(res.getRealUriRequest().getPath()).length == 1;
  }

  @Override
  protected boolean existTmpFile(DataResource res) throws IOException {
    return res.getFileManager().listFiles(res.getTmpUriRequest().getPath()).length == 1;
  }

  @Override
  protected FTPFile getFile(DataResource res, UriRequest uri, boolean useWild) throws IOException {
    FTPClient client = res.getFileManager();
    FtpDataStorageFilter filter = new FtpDataStorageFilter(client.getRemoteAddress().getHostName(),
        uri, useWild);
    client.enterLocalPassiveMode();
    if (client.hasFeature("MLST")) {
      FTPFile f = client.mlistFile(uri.getPath());
      return f != null && filter.accept(f) ? f : null;
    } else {
      LOG.warn("FTP Server doesn't have MLST , so use LIST instead.");
      Path p = new Path(res.getRealUriRequest().getPath());
      if (p.getParent() == null) { // path is root
        return null;
      }
      FTPFile[] ftpFiles = client.listFiles(p.getParent().toString());
      if (ftpFiles != null) {
        for (FTPFile ftpFile : ftpFiles) {
          if (ftpFile.getName().equals(p.getName())) { // file found in dir
            return ftpFile;
          }
        }
      }
      return null;
    }
  }

  @Override
  protected FTPFile[] singletonArray(FTPFile file) {
    return file == null ? new FTPFile[]{} : new FTPFile[]{file};
  }

  @Override
  public long getConnectionNum() {
    return manager.getConnNum();
  }
}
