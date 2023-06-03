package umc.cdc.hds.datastorage.status;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class DataRecordWrapperFromFile extends BaseDataRecordWrapper<File> {

  private final UriRequest parent;

  public DataRecordWrapperFromFile(UriRequest parent) {
    this.parent = parent;
  }

  @Override
  protected String wrapName(File file) {
    return file.getName();
  }

  @Override
  protected long wrapSize(File file) {
    return file.length();
  }

  @Override
  protected long wrapModifiedTime(File file) {
    return file.lastModified();
  }

  @Override
  protected Protocol wrapLocation() {
    return Protocol.file;
  }

  @Override
  protected Map<String, Double> wrapDataOwner(File file) {
    Map<String, Double> dataOwner = new TreeMap<>();
    try {
      dataOwner.put(InetAddress.getLocalHost().getHostName(), 1.0);
    } catch (UnknownHostException ex) {
      dataOwner.put("unknown host", 1.0);
    }
    return dataOwner;
  }

  @Override
  protected FileType wrapFileType(File file) {
    if (file.isFile()) {
      return FileType.file;
    }
    if (file.isDirectory()) {
      return FileType.directory;
    }
    return null;
  }

  @Override
  protected String wrapUri(File file) {
    if (file.isDirectory() && !file.getPath().endsWith("/")) {
      parent.setName(file.getName() + "/");
    } else {
      parent.setName(file.getName());
    }
    return parent.toString();
  }

}
