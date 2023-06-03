package com.dslab.drs.restful.api.response.list;

import com.dslab.drs.restful.api.json.JsonSerialization;
import java.util.List;

/**
 *
 * @author kh87313
 */
public class DataInfos implements JsonSerialization {

  private List<DataInfo> dataInfo;

  public List<DataInfo> getDataInfo() {
    return this.dataInfo;
  }
}
