package com.dslab.drs.restful.api.response.application;

import com.dslab.drs.restful.api.json.JsonSerialization;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author kh87313
 */
public class AppsResult implements JsonSerialization{
   List<AppResult> app;
   
  public List<AppResult> getApp() {
    return this.app;
  }
  
    @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj instanceof AppsResult) {
      AppsResult dst = (AppsResult) obj;
      if (app == null) {
        return dst.app == null;
      }
      if (dst.app == null) {
        return false;
      }
      return app.stream().allMatch(n -> dst.app.contains(n));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + Objects.hashCode(this.app);
    return hash;
  }
}
