package com.dslab.drs.restful.api.response.watch;

import com.dslab.drs.restful.api.json.JsonSerialization;

/**
 *
 * @author kh87313
 */
public class WatchBody implements JsonSerialization {

  private WatchResult applications;

  public WatchResult getApplications() {
    return this.applications;
  }
}
