package com.dslab.drs.restful.api.response.appStatus;

import com.dslab.drs.restful.api.json.JsonSerialization;
import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;

/**
 *
 * @author kh87313
 */
public class AppstateResult implements JsonSerialization {

  @VisibleForTesting
  String state;

  public String getState() {
    return this.state;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj instanceof AppstateResult) {
      AppstateResult dst = (AppstateResult) obj;
      if (state == null) {
        return dst.state == null;
      }
      if (dst.state == null) {
        return false;
      }
      return state == dst.state;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + Objects.hashCode(this.state);
    return hash;
  }
}
