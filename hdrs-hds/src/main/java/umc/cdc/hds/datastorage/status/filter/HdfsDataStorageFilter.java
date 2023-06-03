/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromHdfs;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class HdfsDataStorageFilter extends BaseFilter implements PathFilter {

  private final UriRequest parent;
  private final DataRecordWrapperFromHdfs wrapper;
  private final FileSystem fs;
  private static final Log LOG = LogFactory.getLog(HdfsDataStorageFilter.class);

  public HdfsDataStorageFilter(FileSystem fs, UriRequest parent, boolean useWild) {
    super(useWild);
    this.parent = parent;
    this.fs = fs;
    wrapper = new DataRecordWrapperFromHdfs(parent, fs);

  }

  @Override
  public boolean accept(Path path) {
    try {
      return filt(wrapper.wrapSingleFile(fs.getFileStatus(path)), parent.getQuery());
    } catch (IOException ex) {
      LOG.error(ex);
      return false;
    }
  }

}
