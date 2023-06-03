/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.load;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.metric.MetricSystem;

/**
 *
 * @author brandboat
 */
public class NodeLoadInfo {

  private ServerName serverName;
  private List<OperationLoadInfo> opLoads;

  public NodeLoadInfo() {
  }

  public NodeLoadInfo(Configuration conf, MetricSystem metricSystem) throws IOException {
    String host = conf.get(
        HDSConstants.HTTP_SERVER_ADDRESS,
        InetAddress.getLocalHost().getHostName());
    int port = conf.getInt(
        HDSConstants.HTTP_SERVER_PORT,
        HDSConstants.DEFAULT_HTTP_SERVER_PORT);
    serverName = ServerName.valueOf(host, port, 0);
    opLoads = createAllLoad(metricSystem);
  }

  private List<OperationLoadInfo> createAllLoad(MetricSystem metricSystem) throws IOException {
    List<OperationLoadInfo> l = new LinkedList<>();
    for (HDSConstants.LOADINGCATEGORY category : HDSConstants.LOADINGCATEGORY.values()) {
      l.add(new OperationLoadInfoBuilder()
          .setLoadingCategory(category)
          .setPastCount(
              metricSystem.getAllHistoryRequests(category))
          .setPastBytes(
              metricSystem.getAllHistoryBytes(category))
          .setDealCount(
              metricSystem.getAllFutureRequests(category))
          .setDealBytes(
              metricSystem.getAllFutureBytes(category))
          .build());
    }
    return l;
  }

  public String getHost() {
    return serverName.getHostname();
  }

  public int getPort() {
    return serverName.getPort();
  }

  public ServerName getServerName() {
    return serverName;
  }

  public List<OperationLoadInfo> getOperationLoadInfos() {
    return opLoads;
  }

  public void serialize(DataOutputStream out) throws IOException {
    out.writeUTF(serverName.getHostname());
    out.writeInt(serverName.getPort());
    for (OperationLoadInfo opLoad : opLoads) {
      opLoad.serialize(out);
    }
  }

  public void deserialize(DataInputStream in) throws IOException {
    String host = in.readUTF();
    int port = in.readInt();
    serverName = ServerName.valueOf(host, port, 0);
    opLoads = new LinkedList<>();
    try {
      while (true) {
        OperationLoadInfo opLoad = new OperationLoadInfo();
        opLoad.deserialize(in);
        opLoads.add(opLoad);
      }
    } catch (IOException ex) {
    }
  }
}
