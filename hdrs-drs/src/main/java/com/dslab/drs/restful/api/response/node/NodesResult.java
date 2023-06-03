package com.dslab.drs.restful.api.response.node;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Objects;
import com.dslab.drs.restful.api.json.JsonSerialization;
import java.util.ArrayList;

public class NodesResult implements JsonSerialization {

  @VisibleForTesting
  List<NodeResult> node;

  public List<NodeResult> getNodes() {
    return this.node;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj instanceof NodesResult) {
      NodesResult dst = (NodesResult) obj;
      if (node == null) {
        return dst.node == null;
      }
      if (dst.node == null) {
        return false;
      }
      return node.stream().allMatch(n -> dst.node.contains(n));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + Objects.hashCode(this.node);
    return hash;
  }
}
