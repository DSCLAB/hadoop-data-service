package umc.cdc.hds.datastorage;

import com.jcraft.jsch.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.*;
import umc.cdc.hds.exceptions.SftpDataStorageException;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.UriRequest;

import java.io.*;
import java.util.*;

/**
 *
 * @author jpopaholic
 */
public class SftpDataStorage extends BaseDataStorage<ChannelSftp, ChannelSftp.LsEntry> {

  private SftpResourceManager manager;
  private static final Log LOG = LogFactory.getLog(SftpDataStorage.class);

  public SftpDataStorage(Configuration conf) {
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
  private SftpDataStorage(Configuration conf, int poolSize, int checkTime, long timeout) {
    super(conf);
    manager = new SftpResourceManager(poolSize, checkTime, timeout);
  }

  @Override
  protected DataResource getDataResource(UriRequest uri) throws IOException {
    SftpResourceManager.SftpResource resource = manager.getSftpResource(uri);
    return new DataResource(uri) {
      @Override
      public ChannelSftp getFileManager() {
        return resource.getChannelSftp();
      }

      @Override
      public void close() throws IOException {
        resource.release();
      }
    };
  }
  public boolean isDirExist(ChannelSftp client,String directory)
  {
    try {
      client.ls(directory);
      return true;
    } catch (SftpException e) {
      return false;
    }

  }
  public boolean createDir(ChannelSftp client,String createpath) {
    try
    {
      if (isDirExist(client,createpath)) {
        return true;
      }
      String pathArry[] = createpath.split("/");
      StringBuffer filePath = new StringBuffer("/");
      for (String path : pathArry) {
        if (path.equals("")) {
          continue;
        }
        filePath.append(path+"/");
        if (!isDirExist(client,filePath.toString())) {
          client.mkdir(filePath.toString());
        }
      }
      return true;
    } catch (SftpException e) {
      return false;
    }
  }
  private void createDirectoryIfNotExists(ChannelSftp client, String path) throws IOException {
    if(!createDir(client,path)){
      throw new IOException("Create directory fails."+path);
    }
  }

  @Override
  protected OutputStream initinalizeOutput(DataResource res) throws IOException {
    ChannelSftp client = res.getFileManager();
    try {
      createDirectoryIfNotExists(client, res.getTmpUriRequest().getDir());
      String filePath = res.getTmpUriRequest().getPath();
      return client.put(filePath);
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString());
    }
  }


  @Override
  protected void removeTmpFile(DataResource res) throws IOException {
    ChannelSftp client = res.getFileManager();
    try {
      client.rm(res.getTmpUriRequest().getPath());
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString());
    }
  }

  @Override
  protected void removeRealFile(DataResource res) throws IOException {
    ChannelSftp client = res.getFileManager();
    try {
      client.rm(res.getRealUriRequest().getPath());
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString());
    }
  }

  @Override
  protected void renameFile(DataResource res) throws IOException {
    ChannelSftp client = res.getFileManager();
    try {
      client.rename(res.getTmpUriRequest().getPath(), res.getRealUriRequest().getPath());
    } catch (SftpException e) {
      throw new SftpDataStorageException(HDSConstants.CHANGE_TO_FILE_FAILED+ e);
    }
  }

  @Override
  protected void changeRealFileTime(DataResource res, long time) throws IOException {
    ChannelSftp client = res.getFileManager();
    try {
      client.setMtime(res.getRealUriRequest().getPath(), (int)time);
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString());
    }
  }

  @Override
  protected ChannelSftp.LsEntry[] internalList(DataResource res, boolean useWild) throws IOException {
    ChannelSftp client = res.getFileManager();
    UriRequest uri = res.getRealUriRequest();
    try {
      Vector v = client.ls(uri.getPath());
      int i=0;
      ChannelSftp.LsEntry[] files = new ChannelSftp.LsEntry[v.size()-2];
      for (Object file : v) {
        String name = ((ChannelSftp.LsEntry)file).getFilename();
        if((name.equals(".")) || (name.equals(".."))){ //don't add "." and ".." directory
          continue;
        }
        files[i] = (ChannelSftp.LsEntry)file;
        i++;
      }
      return files;
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString()+uri.getPath());
    }
  }

  @Override
  protected Iterator<DataRecord> wrapFiles(DataResource res, ChannelSftp.LsEntry[] files) throws IOException {
    DataRecordWrapperFromSftp wrapper = null;
    try {
      wrapper = new DataRecordWrapperFromSftp(res.getRealUriRequest(),
          res.getFileManager().getSession().getHost());
    } catch (JSchException e) {
      new SftpDataStorageException(e.toString());
    }
    return wrapper.wrapFiles(files);
  }

  @Override
  protected void deleteRecursive(DataResource res, ChannelSftp.LsEntry deleteFile) throws IOException {
    ChannelSftp client = res.getFileManager();


    if (deleteFile.getAttrs().isDir()) {
      try {
        client.rmdir(res.getRealUriRequest().getPath());
      } catch (SftpException e) {
        throw new SftpDataStorageException(e.toString());
      }
    } else {
      try {
        client.rm(res.getRealUriRequest().getPath());
      } catch (SftpException e) {
        throw new SftpDataStorageException(e.toString());
      }
    }
  }

  @Override
  protected CloseableIterator<BatchDeleteRecord> wrapParticalFilesWithErrorMessage(
      DataResource res,
      ChannelSftp.LsEntry[] files,
      int[] accept,
      List<Exception> exs) {
    String serverName = null;
    try {
      serverName = res.getFileManager().getSession().getHost();
    } catch (JSchException e) {
      new SftpDataStorageException(e.toString());
    }
    DataRecordWrapperFromSftp wrapper = new DataRecordWrapperFromSftp(res.getRealUriRequest(), serverName);
    return wrapper.wrapParticalFilesWithErrorMessage(files, accept, exs);

  }

  @Override
  protected InputStream initinalizeInput(DataResource res) throws IOException {

    ChannelSftp client = res.getFileManager();
    try {
      createDirectoryIfNotExists(client, res.getTmpUriRequest().getDir());
      String filePath = res.getRealUriRequest().getPath();
      return client.get(filePath);
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString());
    }
  }

  @Override
  protected DataRecord getFileRecord(DataResource res) throws IOException {
    String host = null;
    try {
      host = res.getFileManager().getSession().getHost();
    } catch (JSchException e) {
      new SftpDataStorageException(e.toString());
    }
    DataRecordWrapperFromSftp wrapper = new DataRecordWrapperFromSftp(res.getRealUriRequest(), host);
    Path p = new Path(res.getRealUriRequest().getPath());
    Vector f = null;
    ChannelSftp.LsEntry file = null;
    try {
      f = res.getFileManager().ls(res.getRealUriRequest().getPath());
    } catch (SftpException e) {
      new SftpDataStorageException(e.toString());
    }
    for (Object o : f) {
      if((((ChannelSftp.LsEntry) o).getFilename()).equals(p.getName())){
        file=(ChannelSftp.LsEntry) o;
        break;
      }
    }
    return wrapper.wrapSingleFile(file);
  }

  @Override
  public void close() throws IOException {
    manager.close();
  }

  @Override
  protected boolean existRealFile(DataResource res) throws IOException {
    try {
      res.getFileManager().ls(res.getRealUriRequest().getPath());
      return true;
    } catch (SftpException e) {
      return false;
    }
  }

  @Override
  protected boolean existTmpFile(DataResource res) throws IOException {
    try {
      res.getFileManager().ls(res.getTmpUriRequest().getPath());
      return true;
    } catch (SftpException e) {
      return false;
    }
  }

  @Override
  protected ChannelSftp.LsEntry[] singletonArray(ChannelSftp.LsEntry file) {
    return file == null ? new ChannelSftp.LsEntry[]{} : new ChannelSftp.LsEntry[]{file};
  }

  @Override
  protected ChannelSftp.LsEntry getFile(DataResource res, UriRequest uri, boolean useWild) throws IOException {
    ChannelSftp client = res.getFileManager();
    String name;
    try {
      Vector v = client.ls(uri.getDir());
      for (Object file : v) {
        name=((ChannelSftp.LsEntry)file).getFilename();
        if((name.equals(".")) || (name.equals(".."))){continue;}
        if (name.equals(uri.getName().toString())) { // file found in dir
          return (ChannelSftp.LsEntry)file;
        }
      }
      return null;
    } catch (SftpException e) {
      throw new SftpDataStorageException(e.toString());
    }
  }

  @Override
  public long getConnectionNum() {
    return manager.getConnNum();
  }
}
