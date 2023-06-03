package umc.cdc.hds.datastorage.status;

import java.util.Map;
import java.util.TreeMap;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class DataRecordWrapperFromSmb extends BaseDataRecordWrapper<SmbFile> {

  private static final Log LOG = LogFactory.getLog(DataRecordWrapperFromSmb.class);
  private final UriRequest parent;

  public DataRecordWrapperFromSmb(UriRequest parent) {
    this.parent = parent;
  }

  @Override
  protected String wrapName(SmbFile file) {
    return file.getName();
  }

  @Override
  protected long wrapSize(SmbFile file) {
    try {
      return file.length();
    } catch (SmbException ex) {
      LOG.error(ex);
      return 0l;
    }
  }

  @Override
  protected long wrapModifiedTime(SmbFile file) {
    return file.getLastModified();
  }

  @Override
  protected Protocol wrapLocation() {
    return Protocol.smb;
  }

  @Override
  protected Map<String, Double> wrapDataOwner(SmbFile file) {
    Map<String, Double> dataOwner = new TreeMap<>();
    dataOwner.put(file.getServer(), 1.0);
    return dataOwner;
  }

  @Override
  protected FileType wrapFileType(SmbFile file) {
    try {
      if (file.isFile()) {
        return FileType.file;
      }
      if (file.isDirectory()) {
        return FileType.directory;
      }
    } catch (SmbException ex) {
    }
    return null;
  }

  @Override
  protected String wrapUri(SmbFile file) {
    try {
      if (file.isDirectory() && !file.getPath().endsWith("/")) {
        parent.setName(file.getName() + "/");
      } else {
        parent.setName(file.getName());
      }
    } catch (SmbException ex) {
      parent.setName(file.getName());
    }
    return parent.toString();
  }

}
