/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;

import java.util.regex.Pattern;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.HDSUtil;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author jpopaholic
 */
public class BaseFilter {

  private int index;
  private final boolean useWild;

  protected BaseFilter(boolean useWild) {
    index = 0;
    this.useWild = useWild;
  }

  protected boolean filt(DataRecord le, Query queries) {
    return filtName(le, queries.getQueryValue(HDSConstants.HDS_URI_ARG_NAME).orElse(null))
        && filtSize(le,
            queries.getQueryValueAsLong(HDSConstants.HDS_URI_ARG_SIZE, -1l),
            queries.getQueryValueAsLong(HDSConstants.HDS_URI_ARG_MINSIZE, 0),
            queries.getQueryValueAsLong(HDSConstants.HDS_URI_ARG_MAXSIZE, Long.MAX_VALUE))
        && filtTime(le,
            queries.getQueryValueAsTime(HDSConstants.HDS_URI_ARG_TIME, -1l),
            queries.getQueryValueAsTime(HDSConstants.HDS_URI_ARG_MINTIME, 0l),
            queries.getQueryValueAsTime(HDSConstants.HDS_URI_ARG_MAXTIME, Long.MAX_VALUE))
        && filtFile(le, queries.getQueryValueAsBoolean(HDSConstants.DS_URI_ARG_FILE, true))
        && filtDir(le, queries.getQueryValueAsBoolean(HDSConstants.DS_URI_ARG_DIRECTORY, false));
  }

  private boolean filtName(DataRecord le, String wildcard) {
    if (useWild) {
      if (wildcard != null && !wildcard.isEmpty()) {
        return Pattern.matches(HDSUtil.convertWildCardToRegEx(wildcard), le.getName());
      }
    } else if (wildcard != null && !wildcard.isEmpty()) {
      return le.getName().equals(wildcard);
    }
    return true;
  }

  private boolean filtSize(DataRecord le, long absoluteSize, long minSize, long maxSize) {
    if (absoluteSize >= 0) {
      return le.getSize() == absoluteSize;
    }
    return le.getSize() >= minSize && le.getSize() <= maxSize;
  }

  private boolean filtTime(DataRecord le, long absoluteTime, long startTime, long endTime) {
    if (absoluteTime >= 0) {
      return le.getTime() == absoluteTime;
    }
    return le.getTime() >= startTime && le.getTime() <= endTime;
  }

  private boolean filtFile(DataRecord le, boolean filtFile) {
    if (le.getFileType().isPresent()) {
      if (le.getFileType().get().equals(DataRecord.FileType.file)) {
        return filtFile;
      } else {
        return true;
      }
    }
    return false;
  }

  private boolean filtDir(DataRecord le, boolean filtDir) {
    if (le.getFileType().isPresent()) {
      if (le.getFileType().get().equals(DataRecord.FileType.directory)) {
        return filtDir;
      } else {
        return true;
      }
    }
    return false;
  }
}
