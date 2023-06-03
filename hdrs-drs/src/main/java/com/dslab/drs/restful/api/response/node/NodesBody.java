package com.dslab.drs.restful.api.response.node;

import com.dslab.drs.restful.api.json.JsonSerialization;


public class NodesBody implements JsonSerialization {

  private NodesResult nodes;

  public NodesResult getNodes() {
    return this.nodes;
  }
}
