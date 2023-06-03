package com.dslab.drs.restful.api.response.list;

import com.dslab.drs.restful.api.json.JsonSerialization;
import java.util.List;

/**
 *
 * @author kh87313
 */
public class DataInfo implements JsonSerialization{
    private String uri;
    private String location;
    private String name;
    private String size;
    private String ts;
    private String type;
    private List<DataOwner> dataowner;

    public String getUri() {
      return this.uri;
    }

    public String getLocation() {
      return this.location;
    }

    public String getName() {
      return this.name;
    }

    public String getSize() {
      return this.size;
    }

    public String getTs() {
      return this.ts;
    }

    public String getType() {
      return this.type;
    }

    public List<DataOwner> getDataowner() {
      return this.dataowner;
    }
}
