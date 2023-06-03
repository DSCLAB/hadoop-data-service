/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.load;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.tools.ThreadPoolManager;
import umc.cdc.hds.zookeeper.ZKChildrenMonitorExecutor;
import umc.cdc.hds.zookeeper.ZKDataMonitorExecutor;
import umc.cdc.hds.zookeeper.ZNodePath;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public class LoadingListener implements Closeable {

  private final static String ZK_SERVERS = "/hds/servers";
  private final static Map<String, DataMonitor> nodeListener = new ConcurrentHashMap<>();
  private final static Log LOG = LogFactory.getLog(LoadingListener.class);
  private final Configuration conf;
  private final ChildrenMonitor childMon;

  private LoadingListener(Configuration conf) throws Exception {
    this.conf = conf;
    childMon = new ChildrenMonitor(conf, new ZNodePath(ZK_SERVERS));
    ThreadPoolManager.execute(childMon);
  }

  public static int getServerNum() {
    return nodeListener.entrySet().size();
  }

  public Iterator<NodeLoadInfo> getLoading() {
    List<NodeLoadInfo> loading = new ArrayList<>();
    for (DataMonitor dm : nodeListener.values()) {
      try (ByteArrayInputStream b = new ByteArrayInputStream(dm.getData())) {
        try (DataInputStream d = new DataInputStream(b)) {
          NodeLoadInfo nl = new NodeLoadInfo();
          nl.deserialize(d);
          loading.add(nl);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    loading.sort((NodeLoadInfo o1, NodeLoadInfo o2) -> {
      if (o1.getHost().compareToIgnoreCase(o2.getHost()) >= 0) {
        return 1;
      } else {
        return -1;
      }
    });
    return loading.iterator();
  }

  public static ShareableObject<LoadingListener> getInstance(Configuration conf)
      throws Exception {
    return ShareableObject.<LoadingListener>create(
        (Object obj) -> obj instanceof LoadingListener,
        () -> new LoadingListener(conf));
  }

  @Override
  public void close() throws IOException {
  }

  private class ChildrenMonitor extends ZKChildrenMonitorExecutor {

    private List<String> node = new LinkedList<>();

    public ChildrenMonitor(Configuration conf, ZNodePath zn) throws Exception {
      super(conf, zn);
    }

    @Override
    public void exists(List<String> children) {
      setData(children);
    }

    @Override
    public void dataChanged(List<String> children) {
      setData(children);
    }

    private void setData(List<String> nodes) {
      if (nodes == null) {
        node = new LinkedList<>();
      } else {
        node = nodes;
      }
      // clear nonexist node
      Set<String> prevNodes = nodeListener.keySet();
      prevNodes.forEach(p -> {
        if (!node.contains(p)) {
          nodeListener.remove(p);
        }
      });
      // add node that isn't in nodeListener.
      nodes.forEach(n -> {
        if (!nodeListener.containsKey(n)) {
          try {
            DataMonitor dm = new DataMonitor(
                conf,
                new ZNodePath(ZK_SERVERS + "/" + n));
            nodeListener.putIfAbsent(n, dm);
            ThreadPoolManager.execute(dm);
          } catch (Exception ex) {
            LOG.error(ex);
          }
        }
      });
    }

    public List<String> getNodes() {
      return node;
    }

  }

  private class DataMonitor extends ZKDataMonitorExecutor {

    private byte[] data = new byte[0];

    public DataMonitor(Configuration conf, ZNodePath zn) throws Exception {
      super(conf, zn);
    }

    @Override
    public void exists(byte[] d) {
      setData(d);
    }

    @Override
    public void dataChanged(byte[] d) {
      setData(d);
    }

    private void setData(byte[] d) {
      if (d == null) {
        data = new byte[0];
      } else {
        data = d;
      }
    }

    public byte[] getData() {
      return data;
    }

  }
}
