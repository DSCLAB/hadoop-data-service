/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromFtp;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class FtpDataStorageFilter extends BaseFilter implements FTPFileFilter {

  private final UriRequest parent;
  private final DataRecordWrapperFromFtp wrapper;

  public FtpDataStorageFilter(String serverName, UriRequest parent, boolean useWild) {
    super(useWild);
    this.parent = parent;
    wrapper = new DataRecordWrapperFromFtp(parent, serverName);

  }

  @Override
  public boolean accept(FTPFile file) {
    return filt(wrapper.wrapSingleFile(file), parent.getQuery());
  }
}
