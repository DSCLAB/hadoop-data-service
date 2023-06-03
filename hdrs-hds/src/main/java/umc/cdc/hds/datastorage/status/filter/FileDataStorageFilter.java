/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;

import java.io.File;
import java.io.FileFilter;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromFile;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class FileDataStorageFilter extends BaseFilter implements FileFilter {

  private final UriRequest parent;
  private final DataRecordWrapperFromFile wrapper;

  public FileDataStorageFilter(UriRequest parent, boolean useWild) {
    super(useWild);
    this.parent = parent;
    wrapper = new DataRecordWrapperFromFile(parent);
  }

  @Override
  public boolean accept(File pathname) {
    return filt(wrapper.wrapSingleFile(pathname), parent.getQuery());
  }

}
