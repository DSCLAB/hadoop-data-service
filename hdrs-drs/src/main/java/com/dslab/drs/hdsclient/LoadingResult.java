package com.dslab.drs.hdsclient;

import java.util.List;
import com.dslab.drs.restful.api.json.JsonSerialization;

public class LoadingResult implements JsonSerialization {

  private List<LoadingContent> loading;

  public List<LoadingContent> getLoading() {
    return this.loading;
  }

}
