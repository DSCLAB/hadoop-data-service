package umc.cdc.hds.datastorage.status;

import com.jcraft.jsch.ChannelSftp;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTPFile;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

/**
 *
 * @author jpopaholic
 */
public class DataRecordWrapperFromSftp extends BaseDataRecordWrapper<ChannelSftp.LsEntry> {

  private final UriRequest parent;
  private final String serverName;

  public DataRecordWrapperFromSftp(UriRequest parent, String serverName) {
    this.parent = parent;
    this.serverName = serverName;
  }

  @Override
  protected String wrapName(ChannelSftp.LsEntry file) {
    Object f = file;
    String filename = ((ChannelSftp.LsEntry) f).getFilename();
    return filename;
  }

  @Override
  protected long wrapSize(ChannelSftp.LsEntry file) {
    Object f = file;
    if (((ChannelSftp.LsEntry) f).getAttrs().isDir()) {
      return 0l;
    }
    return ((ChannelSftp.LsEntry) f).getAttrs().getSize();
  }

  @Override
  protected long wrapModifiedTime(ChannelSftp.LsEntry file) {
    Object f = file;
    return ((ChannelSftp.LsEntry) f).getAttrs().getMTime();
  }

  @Override
  protected Protocol wrapLocation() {
    return Protocol.sftp;
  }

  @Override
  protected Map<String, Double> wrapDataOwner(ChannelSftp.LsEntry file) {
    Map<String, Double> dataOwner = new TreeMap<>();
    dataOwner.put(serverName, 1.0);
    return dataOwner;
  }

  @Override
  protected FileType wrapFileType(ChannelSftp.LsEntry file) {
    Object f = file;
    if (((ChannelSftp.LsEntry) f).getAttrs().isDir()) {
      return FileType.directory;
    }else{
      return FileType.file;
    }
  }

  @Override
  protected String wrapUri(ChannelSftp.LsEntry file) {
    try {
      UriRequest uri = UriParser.valueOf(parent);
      Object f = file;
      if (!((ChannelSftp.LsEntry) f).getAttrs().isDir()) {
        uri.setName(FilenameUtils.getName(((ChannelSftp.LsEntry) f).getFilename()));
      } else {
        StringBuilder str = new StringBuilder();
        str.append(parent.getDir());
        str.append(FilenameUtils.getName(((ChannelSftp.LsEntry) f).getFilename()));
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
