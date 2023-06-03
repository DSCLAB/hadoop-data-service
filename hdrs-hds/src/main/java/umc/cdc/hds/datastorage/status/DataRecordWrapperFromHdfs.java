package umc.cdc.hds.datastorage.status;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import umc.cdc.hds.core.HDSUtil;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class DataRecordWrapperFromHdfs extends BaseDataRecordWrapper<FileStatus> {

  private final UriRequest parent;
  private final FileSystem hdfs;

  public DataRecordWrapperFromHdfs(UriRequest parent, FileSystem hdfs) {
    this.parent = parent;
    this.hdfs = hdfs;
  }

  @Override
  protected String wrapName(FileStatus file) {
    return file.getPath().getName();
  }

  @Override
  protected long wrapSize(FileStatus file) {
    return file.getLen();
  }

  @Override
  protected long wrapModifiedTime(FileStatus file) {
    return file.getModificationTime();
  }

  @Override
  protected Protocol wrapLocation() {
    return Protocol.hdfs;
  }

  @Override
  protected Map<String, Double> wrapDataOwner(FileStatus file) {
    Map<String, Integer> map = new TreeMap<>();
    if (file.isFile()) {
      try {
        return HDSUtil.getHdfsDataLocationPercentage(hdfs, file.getPath().toString());
      } catch (IOException ie) {
        return new HashMap();
      }
    }
    return new HashMap();
  }

  @Override
  protected FileType wrapFileType(FileStatus file) {
    if (file.isFile()) {
      return FileType.file;
    }
    if (file.isDirectory()) {
      return FileType.directory;
    }
    return null;
  }

  @Override
  protected String wrapUri(FileStatus file) {
    if (file.isDirectory() && !file.getPath().toString().endsWith("/")) {
      parent.setName(file.getPath().getName() + "/");
    } else {
      parent.setName(file.getPath().getName());
    }
    return parent.toString();
  }

}
