/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public class ZKAccessor implements Closeable {

  private ZooKeeper zk;
  private final ZKAuth auth;
  private final Configuration conf;
  private static final Log LOG = LogFactory.getLog(ZKAccessor.class);
  private static final String ZK_HDS_ROOT_DIR = "/hds";

  private ZKAccessor(Configuration conf) throws Exception {
    this.conf = conf;
    auth = new ZKAuth();
    buildZKConn(conf);
    createHdsRootNode();
  }

  public static ShareableObject<ZKAccessor> getInstance(Configuration conf)
      throws Exception {
    return ShareableObject.<ZKAccessor>create(
        (Object obj) -> obj instanceof ZKAccessor,
        () -> new ZKAccessor(conf));
  }

  public ZooKeeper getZK() {
    return zk;
  }

  public void createNode(ZNodePath p, byte[] data) throws KeeperException, InterruptedException {
    String path = p.getPath();
    path = path.substring(1, path.length());
    String[] array = path.split("/");
    String acum = "";
    for (int i = 0; i < array.length - 2; i++) {
      acum += ("/" + array[i]);
      createPersistentNodeIfNotExist(acum, new byte[0]);
    }
    acum += ("/" + array[array.length - 2]);
    createPersistentNodeIfNotExist(acum, data);
    acum += ("/" + array[array.length - 1]);
    createEphemeralNode(acum);
  }

  public String createSequenceNode(ZNodePath p, byte[] data) throws KeeperException, InterruptedException {
    String path = p.getPath();
    path = path.substring(1, path.length());
    String[] array = path.split("/");
    String acum = "";
    for (int i = 0; i < array.length - 2; i++) {
      acum += ("/" + array[i]);
      createPersistentNodeIfNotExist(acum, new byte[0]);
    }
    acum += ("/" + array[array.length - 2]);
    createPersistentNodeIfNotExist(acum, data);
    acum += ("/" + array[array.length - 1]);
    return createSequenceEphemeralNode(acum);
  }

  public void setNodeData(ZNodePath path, byte[] b)
      throws KeeperException, InterruptedException {
    zk.setData(path.getPath(), b, -1);
  }

  public byte[] getNodeData(ZNodePath path)
      throws KeeperException, InterruptedException {
    return zk.getData(path.getPath(), false, new Stat());
  }

  private void buildZKConn(Configuration conf) throws IOException {
    zk = new ZooKeeper(conf.get("hbase.zookeeper.quorum"),
        conf.getInt("zookeeper.session.timeout", 180000),
        new DefaultZKWatcher());
    zk.addAuthInfo(auth.getScheme(), auth.getAuth());
  }

  private void createPersistentNodeIfNotExist(String path, byte[] data)
      throws KeeperException, InterruptedException {
    createZKNodeIfNotExist(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Create ephemeral node, If there already has the node, delete it first then
   * create it again.
   *
   * @param path
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void createEphemeralNode(String path)
      throws KeeperException, InterruptedException {
    if (zk.exists(path, null) == null) {
      createZKNode(path, CreateMode.EPHEMERAL);
    } else {
      deleteNode(path);
      createZKNode(path, CreateMode.EPHEMERAL);
    }
  }

  private String createSequenceEphemeralNode(String path)
      throws KeeperException, InterruptedException {
    return createZKNode(path, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  private void createZKNodeIfNotExist(String path, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {
    if (zk.exists(path, null) == null) {
      try {
        zk.create(path,
            data,
            auth.getACL(),
            mode);
      } catch (KeeperException ex) {
        if (ex instanceof KeeperException.NodeExistsException) {
        } else {
          throw ex;
        }
      }
    }
  }

  private String createZKNode(String path, CreateMode mode)
      throws KeeperException, InterruptedException {
    try {
      return zk.create(path, new byte[0], auth.getACL(), mode);
    } catch (KeeperException ex) {
      throw ex;
    }
  }

  private void deleteNode(String path)
      throws InterruptedException, KeeperException {
    zk.delete(path, -1);
  }

  private void createHdsRootNode()
      throws KeeperException, InterruptedException {
    createPersistentNodeIfNotExist(ZK_HDS_ROOT_DIR, new byte[0]);
  }

  @Override
  public void close() throws IOException {
    try {
      zk.close();
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  private class DefaultZKWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      switch (event.getState()) {
        case Expired: {
          LOG.error("zookeeper session expired.");
          try {
            buildZKConn(conf);
          } catch (IOException ex) {
            LOG.error("zookeeper connection creation retry failed.", ex);
          }
        }
        break;
        case AuthFailed:
          LOG.error("zookeeper authfailed.");
          break;
        default:
      }
    }
  }

  public List<String> getChildren(ZNodePath path)
      throws KeeperException, InterruptedException {
    return zk.getChildren(path.getPath(), false);
  }

  public boolean isNodeExist(ZNodePath path)
      throws KeeperException, InterruptedException {
    return zk.exists(path.getPath(), false) != null;
  }

  public void deleteNode(ZNodePath path)
      throws InterruptedException, KeeperException {
    deleteNode(path.getPath());
  }

  public void clearChildern(ZNodePath path, byte[] serverName)
      throws KeeperException, InterruptedException {
    if (zk.exists(path.getPath(), false) != null) {
      zk.getChildren(path.getPath(), false,
          (rc, parentPath, ctx, children) -> {
            children.stream().forEach((child) -> {
              try {
                StringBuilder builder = new StringBuilder();
                builder.append(parentPath).append("/").append(child);
                if (zk.exists(builder.toString(), false) != null
                    && zk.getChildren(builder.toString(), false).isEmpty()
                    && Bytes.equals(zk.getData(builder.toString(), false, new Stat()), serverName)) {
                  zk.delete(builder.toString(), -1);
                }
              } catch (KeeperException | InterruptedException ex) {
                LOG.error(ex);
              }
            });
          }, new Object());
    }
  }
}
