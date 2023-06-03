package com.dslab.drs.restful.api.response.node;

import com.dslab.drs.restful.api.response.node.NodeResult;
import com.dslab.drs.restful.api.response.node.NodesResult;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import com.dslab.drs.exception.GetJsonBodyException;
import com.dslab.drs.restful.api.json.JsonUtils;

public class TestNodesResult {
  
  @Test
  public void testEquals() throws GetJsonBodyException {
   NodeResult node0 = new NodeResult();
   node0.availMemoryMB = 10;
   NodesResult ori = new NodesResult();
   ori.node = new ArrayList<>();
   ori.node.add(node0);
   NodesResult copy = JsonUtils.fromJson(JsonUtils.toJson(ori), NodesResult.class);
   assertEquals(ori, copy);
   NodeResult node1 = new NodeResult();
   node1.id = "xxx";
   ori.node.add(node1);
   assertNotEquals(ori, copy);
  }
}
