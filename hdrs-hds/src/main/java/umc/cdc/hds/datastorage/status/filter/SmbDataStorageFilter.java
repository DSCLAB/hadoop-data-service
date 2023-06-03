/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;

import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileFilter;
import umc.cdc.hds.datastorage.status.DataRecordWrapperFromSmb;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class SmbDataStorageFilter extends BaseFilter implements SmbFileFilter {

  private final UriRequest parent;
  private final DataRecordWrapperFromSmb wrapper;

  public SmbDataStorageFilter(UriRequest parent, boolean useWild) {
    super(useWild);
    this.parent = parent;
    wrapper = new DataRecordWrapperFromSmb(parent);
  }

  @Override
  public boolean accept(SmbFile file) throws SmbException {
    return filt(wrapper.wrapSingleFile(file), parent.getQuery());
  }

}
