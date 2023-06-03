package umc.cdc.hds.fs;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.tools.CloseableIterator;

public class HDSFileStatus extends FileStatus {

  private final Protocol location;

  // directory
  public HDSFileStatus(long modification, Path path) {
    super(0, true, 1, 0, modification, path);
    this.location = Protocol.hbase;
  }

  // file
  public HDSFileStatus(long length, long blocksize, long modification, Path path,
          Protocol location) {
    super(length, false, 1, blocksize, modification, path);
    this.location = location;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  public Protocol getLocation() {
    return this.location;
  }

  public static FileStatus[] getFileStatus(CloseableIterator<DataRecord> it, long blocksize) {
    final List<FileStatus> result = new ArrayList<>();
    DataRecord record;
    while (it.hasNext()) {
      record = it.next();
      if (record.getFileType().isPresent()) {
        if (record.getFileType().get().equals(DataRecord.FileType.file)) {
          result.add(new HDSFileStatus(record.getSize(), blocksize, record.getTime(),
                  new Path(record.getUri()), record.getLocation()));
        } else {
          result.add(new HDSFileStatus(record.getTime(), new Path(record.getUri())));
        }
      }
    }
    return result.toArray(new FileStatus[result.size()]);
  }
}
