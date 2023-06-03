/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.requestloadbalancer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.commons.logging.LogFactory;
import umc.cdc.hds.load.ClusterLoad;
import umc.cdc.hds.load.ClusterLoadImpl;
import umc.cdc.hds.load.NodeLoadInfo;

/**
 *
 * @author jpopaholic
 */
public class HDSRequestLoadBalancer extends AbstractRequestLoadBalancer {

  private final RequestCostsCalculator calculator;
  private static final Log LOG = LogFactory.getLog(HDSRequestLoadBalancer.class);
  private final ClusterLoad loading;

  public HDSRequestLoadBalancer(Configuration conf) throws IOException, Exception {
    super(conf);
    calculator = new RequestCostsCalculator(conf);
    loading = ClusterLoadImpl.getInstance(conf).get();
  }

  @Override
  protected double getHdsServerCost(Request request, ServerName server, Map<ServerName, NodeLoadInfo> allNodeInfo) throws IOException {
    double result = 0;
    switch (request.getAction()) {
      case ACCESS:
        result = calculator.getCost(server, allNodeInfo.get(server));
        LOG.info("server \"" + server.getHostAndPort() + "\" now cost is " + result);
        break;

      default:

    }
    return result;

  }

  @Override
  protected ServerName chooseServer(List<ServerName> servers) throws IOException {
    Random random = new Random(System.nanoTime());
    return servers.get(random.nextInt(servers.size()));
  }

  @Override
  protected Map<ServerName, NodeLoadInfo> getAllNodeInfo() throws IOException {
    Iterator<NodeLoadInfo> infos = loading.getClusterLoading();
    Map<ServerName, NodeLoadInfo> result = new HashMap();
    while (infos.hasNext()) {
      NodeLoadInfo info = infos.next();
      result.put(info.getServerName(), info);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    loading.close();
  }

}
