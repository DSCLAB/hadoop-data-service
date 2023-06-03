/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.load;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.zookeeper.KeeperException;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.metric.HDSMetricSystem;
import umc.cdc.hds.metric.MetricSystem;
import umc.cdc.hds.tools.ThreadPoolManager;
import umc.cdc.hds.zookeeper.ZKAccessor;
import umc.cdc.hds.zookeeper.ZNodePath;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public final class ClusterLoadImpl implements ClusterLoad {

  private final ShareableObject<MetricSystem> metricSystem;
  private final long updatePeriod;
  private final Configuration conf;
  private static final Log LOG = LogFactory.getLog(ClusterLoadImpl.class);
  private final ShareableObject<ZKAccessor> zk;
  private final ServerName serverName;
  private final ZNodePath zkNodePath;
  private final ShareableObject<LoadingListener> listener;

  private ClusterLoadImpl(Configuration conf) throws Exception {
    this.serverName = ServerName.valueOf(
        InetAddress.getLocalHost().getHostName(),
        conf.getInt(HDSConstants.HTTP_SERVER_PORT,
            HDSConstants.DEFAULT_HTTP_SERVER_PORT), 0);
    this.zkNodePath = new ZNodePath("/hds/servers/"
        + serverName.getServerName());
    this.conf = conf;
    this.metricSystem = HDSMetricSystem.getInstance(conf);
    this.updatePeriod = TimeUnit.SECONDS.toMillis(conf.getLong(
        HDSConstants.HDS_METRIC_UPDATE_PERIOD,
        HDSConstants.DEFAULT_HDS_METRIC_UPDATE_PERIOD));
    this.zk = ZKAccessor.getInstance(conf);
    createServerNodeAndSetData();
    this.listener = LoadingListener.getInstance(conf);
    setThreads();
  }

  public static ShareableObject<ClusterLoad> getInstance(Configuration conf)
      throws Exception {
    return ShareableObject.<ClusterLoad>create(
        (Object obj) -> obj instanceof ClusterLoadImpl,
        () -> new ClusterLoadImpl(conf));
  }

  @Override
  public Iterator<NodeLoadInfo> getClusterLoading() throws RuntimeException {
    return listener.get().getLoading();
  }

  @Override
  public void close() throws IOException {
    try {
      zk.close();
    } catch (Exception ex) {
      LOG.error("ZKAccess close failed.", ex);
    }
    try {
      metricSystem.close();
    } catch (Exception ex) {
      LOG.error("MetricSystem close failed.", ex);
    }
    try {
      listener.close();
    } catch (Exception ex) {
      LOG.error("Loading Listener close failed", ex);
    }
  }

  /**
   * Set data for the node that is just created. So we don't need to worry about
   * we would get error when get data from that znode has null data.
   *
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   */
  private void createServerNodeAndSetData()
      throws KeeperException, InterruptedException, IOException {
    try {
      zk.get().createNode(zkNodePath, new byte[0]);
    } catch (KeeperException ex) {
      if (!KeeperException.Code.NODEEXISTS.equals(ex.code())) {
        throw ex;
      }
    }
    updateLoading();
  }

  private void setThreads() {
    ThreadPoolManager.scheduleWithFixedDelay(() -> {
      try {
        updateLoading();
      } catch (Exception ex) {
        LOG.error("Loading update thread was interrupted.");
      }
    }, updatePeriod, TimeUnit.MILLISECONDS);
  }

  private void updateLoading()
      throws KeeperException, InterruptedException, IOException {
    zk.get().setNodeData(zkNodePath, convertNodeLoadInfoToByteArray());
  }

  private byte[] convertNodeLoadInfoToByteArray() throws IOException {
    try (ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream()) {
      try (DataOutputStream out = new DataOutputStream(byteArrayOut)) {
        NodeLoadInfo nodeLoadInfo = new NodeLoadInfo(conf, metricSystem.get());
        nodeLoadInfo.serialize(out);
        return byteArrayOut.toByteArray();
      }
    }
  }

}
