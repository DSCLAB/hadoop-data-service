package com.dslab.drs.restful.api.response.node;

import com.dslab.drs.restful.api.response.node.NodeResult;
import org.junit.Test;
import static org.junit.Assert.*;
import com.dslab.drs.exception.GetJsonBodyException;
import com.dslab.drs.restful.api.json.JsonUtils;

public class TestNodeResult {
  
  @Test
  public void testEquals() throws GetJsonBodyException {
   NodeResult ori = new NodeResult();
   evaluateObject(ori, "initialing obejct");
   ori.availMemoryMB = 10;
   evaluateObject(ori, "availMemoryMB");
   ori.availableVirtualCores = 10;
   evaluateObject(ori, "availableVirtualCores");
   ori.numContainers = 10;
   evaluateObject(ori, "numContainers");
   ori.usedMemoryMB = 10;
   evaluateObject(ori, "usedMemoryMB");
   ori.usedVirtualCores = 10;
   evaluateObject(ori, "usedVirtualCores");
   ori.healthReport = "healthReport";
   evaluateObject(ori, "healthReport");
   ori.id = "id";
   evaluateObject(ori, "id");
   ori.lastHealthUpdate = "lastHealthUpdate";
   evaluateObject(ori, "lastHealthUpdate");
   ori.nodeHTTPAddress = "nodeHTTPAddress";
   evaluateObject(ori, "nodeHTTPAddress");
   ori.nodeHostName = "nodeHostName";
   evaluateObject(ori, "nodeHostName");
   ori.rack = "rack";
   evaluateObject(ori, "rack");
   ori.state = "state";
   evaluateObject(ori, "state");
   ori.version = "version";
   evaluateObject(ori, "version");
  }

  private static void evaluateObject(NodeResult ori, String memberName) throws GetJsonBodyException {
    assertEquals("fail is caused by " + memberName,
      ori, JsonUtils.fromJson(JsonUtils.toJson(ori), NodeResult.class));
  }
}
