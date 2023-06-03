/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status.filter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author jpopaholic
 */
public class SortAndLimitFilter {

  private static Iterator<DataRecord> listElementSort(Iterator<DataRecord> files, String compareTag, boolean asc) {
    List<DataRecord> collect = new ArrayList();
    while (files.hasNext()) {
      collect.add(files.next());
    }
    collect.sort(new ListElementCompare(compareTag, asc));
    return collect.iterator();
  }

  private static class ListElementCompare implements Comparator<DataRecord> {

    private final String flag;
    private final boolean asc;

    public ListElementCompare(String flag, boolean asc) {
      if (flag != null) {
        this.flag = flag.toLowerCase();
      } else {
        this.flag = "";
      }
      this.asc = asc;
    }

    @Override
    public int compare(DataRecord o1, DataRecord o2) {
      switch (flag) {
        case "time":
          return asc ? Long.compare(o1.getTime(), o2.getTime())
              : -Long.compare(o1.getTime(), o2.getTime());
        case "name":
          return asc ? o1.getName().compareTo(o2.getName())
              : -o1.getName().compareTo(o2.getName());
        case "size":
          return asc ? Long.compare(o1.getSize(), o2.getSize())
              : -Long.compare(o1.getSize(), o2.getSize());
        case "location":
          return asc ? o1.getLocation().compareTo(o2.getLocation())
              : -o1.getLocation().compareTo(o2.getLocation());
        case "type":
          int result = o1.getFileType().orElse(DataRecord.FileType.file)
              .compareTo(o2.getFileType().orElse(DataRecord.FileType.file));
          return asc ? result : -result;
        case "path":
          return asc ? o1.getUri().compareTo(o2.getUri())
              : -o1.getUri().compareTo(o2.getUri());
        default:
          return o1.getUri().compareTo(o2.getUri());
      }
    }

  }

  private static CloseableIterator<DataRecord> listElementLimit(Iterator<DataRecord> files, int offset, int limit) {

    for (int i = 0; i < offset && files.hasNext(); i++) {
      files.next();
    }
    return new CloseableIterator<DataRecord>() {
      int currentIndex = offset;

      @Override
      public void close() {
      }

      @Override
      public boolean hasNext() {
        return files.hasNext() && (currentIndex < offset + limit || limit < 0);
      }

      @Override
      public DataRecord next() {
        if (!this.hasNext()) {
          return null;
        }
        currentIndex++;
        return files.next();
      }
    };
  }

  public static CloseableIterator<DataRecord> sortAndLimit(Iterator<DataRecord> rawFiles, Query optArg) {
    Iterator<DataRecord> middle;
    middle = listElementSort(rawFiles,
        optArg.getQueryValue(HDSConstants.URL_ARG_ORDERBY).orElse(null),
        optArg.getQueryValueAsBoolean(HDSConstants.URL_ARG_ASC, true));
    return listElementLimit(middle, optArg.getQueryValueAsInteger(HDSConstants.OFFSET, 0),
        optArg.getQueryValueAsInteger(HDSConstants.LIMIT, HDSConstants.DEFAULT_LIST_LIMIT));
  }

}
