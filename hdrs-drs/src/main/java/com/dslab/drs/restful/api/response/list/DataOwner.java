package com.dslab.drs.restful.api.response.list;
import com.dslab.drs.restful.api.json.JsonSerialization;

/**
 *
 * @author kh87313
 */
public class DataOwner implements JsonSerialization{
    private String host;
    private String ratio;

    public String getHost() {
      return this.host;
    }

    public String getRatio() {
      return this.ratio;
    }
}
