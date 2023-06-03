package com.dslab.drs.restful.api.response.application;

import com.dslab.drs.restful.api.json.JsonSerialization;

/**
 *
 * @author kh87313
 */
public class AppsBody implements JsonSerialization {

  private AppsResult apps;

  public AppsResult getApps() {
    return this.apps;
  }

}
