package umc.cdc.hds.datastorage.status;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTPFile;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class DataRecordWrapperFromFtp extends BaseDataRecordWrapper<FTPFile> {

  private final UriRequest parent;
  private final String serverName;

  public DataRecordWrapperFromFtp(UriRequest parent, String serverName) {
    this.parent = parent;
    this.serverName = serverName;
  }

  @Override
  protected String wrapName(FTPFile file) {
    return FilenameUtils.getName(file.getName());
  }

  @Override
  protected long wrapSize(FTPFile file) {
    if (file.isDirectory()) {
      return 0l;
    }
    return file.getSize();
  }

  @Override
  protected long wrapModifiedTime(FTPFile file) {
    return file.getTimestamp().getTimeInMillis();
  }

  @Override
  protected Protocol wrapLocation() {
    return Protocol.ftp;
  }

  @Override
  protected Map<String, Double> wrapDataOwner(FTPFile file) {
    Map<String, Double> dataOwner = new TreeMap<>();
    dataOwner.put(serverName, 1.0);
    return dataOwner;
  }

  @Override
  protected FileType wrapFileType(FTPFile file) {
    if (file.isFile()) {
      return FileType.file;
    }
    if (file.isDirectory()) {
      return FileType.directory;
    }
    return null;
  }

  @Override
  protected String wrapUri(FTPFile file) {
    try {
      UriRequest uri = UriParser.valueOf(parent);
      if (!file.isDirectory()) {
        uri.setName(FilenameUtils.getName(file.getName()));
      } else {
        StringBuilder str = new StringBuilder();
        str.append(parent.getDir());
        str.append(FilenameUtils.getName(file.getName()));
        if (!str.toString().endsWith("/")) str.append("/");
        uri.setDir(str.toString());
        uri.setName(null);
        return uri.toString();
      }
      return uri.toString();
    } catch (IOException ex) {
      return parent.toString();
    }
  }

}
