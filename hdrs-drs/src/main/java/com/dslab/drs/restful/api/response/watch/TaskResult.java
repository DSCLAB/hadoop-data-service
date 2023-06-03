package com.dslab.drs.restful.api.response.watch;

import com.dslab.drs.restful.api.json.JsonSerialization;

/**
 *
 * @author kh87313
 */
public class TaskResult implements JsonSerialization {

  String url;
  long size;
  String taskStatus;

  public String getUrl() {
    return url;
  }

  public long getSize() {
    return size;
  }

  public String getTaskStatus() {
    return taskStatus;
  }
}
