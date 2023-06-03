/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.requestloadbalancer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.load.NodeLoadInfo;

/**
 *
 * @author jpopaholic
 */
public abstract class AbstractRequestLoadBalancer implements RequestLoadBalancer {

  private final int maxStep;

  public AbstractRequestLoadBalancer(Configuration conf) throws IOException {
    maxStep = conf.getInt(HDSConstants.BALANCER_COMAPARE_MAX_NUM,
        HDSConstants.DEFAULT_BALANCER_COMAPARE_MAX_NUM);
  }

  @Override
  public RequestRedirectPlan balanceRequest(Request originRequest) throws IOException {
    double currentCost = Double.MAX_VALUE;
    Map<ServerName, NodeLoadInfo> allNodeInfo = getAllNodeInfo();
    int nowStep = 0;
    RequestRedirectPlan plan = new RequestRedirectPlan(originRequest.getServerName());
    Map<ServerName, Boolean> choosedServer = getHdsServers(allNodeInfo);
    while (nowStep < Math.min(maxStep, choosedServer.size())) {
      ServerName newServer = chooseServer(choosedServer.entrySet().stream()
          .filter((oneServer) -> !oneServer.getValue())
          .collect(() -> new ArrayList<>(),
              (list, oneServer) -> list.add(oneServer.getKey()),
              (list1, list2) -> list1.addAll(list2)));
      choosedServer.put(newServer, Boolean.TRUE);
      double newCost = getHdsServerCost(originRequest, newServer, allNodeInfo);
      if (newCost < currentCost) {
        currentCost = newCost;
        plan.setRedirectServer(newServer);
      }
      nowStep++;
    }
    return plan;
  }

  private Map<ServerName, Boolean> getHdsServers(Map<ServerName, NodeLoadInfo> allNodeInfo) throws IOException {
    Map<ServerName, Boolean> result = new HashMap();
    allNodeInfo.keySet().forEach((name) -> result.put(name, Boolean.FALSE));
    return result;
  }

  protected abstract Map<ServerName, NodeLoadInfo> getAllNodeInfo() throws IOException;

  protected abstract double getHdsServerCost(Request request, ServerName server, Map<ServerName, NodeLoadInfo> allNodeInfo) throws IOException;

  protected abstract ServerName chooseServer(List<ServerName> availableServers) throws IOException;

  @Override
  public void close() throws IOException {
    //do nothing
  }

}
