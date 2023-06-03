/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.zookeeper;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import static org.apache.zookeeper.KeeperException.Code.NOAUTH;
import static org.apache.zookeeper.KeeperException.Code.NONODE;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 * @author brandboat
 */
public class ChildrenMonitor implements Monitor, Watcher, ChildrenCallback {

  private final ZooKeeper zk;
  private final ZNodePath znode;
  private final Watcher chainedWatcher;
  private boolean dead = false;
  private final ChildrenMonitorListener listener;
  private List<String> prevData;
  private static final Log LOG = LogFactory.getLog(ChildrenMonitor.class);

  public ChildrenMonitor(ZooKeeper zk, ZNodePath znode, Watcher chainedWatcher,
      ChildrenMonitorListener listener) {
    this.zk = zk;
    this.znode = znode;
    this.chainedWatcher = chainedWatcher;
    this.listener = listener;
    // Get things started by checking if the node exists. We are going
    // to be completely event driven
    zk.getChildren(znode.getPath(), this, this, null);
  }

  @Override
  public void processResult(int rc, String path, Object ctx, List<String> children) {
    boolean exists;
    switch (Code.get(rc)) {
      case OK:
        exists = true;
        break;
      case NONODE:
        exists = false;
        break;
      case SESSIONEXPIRED:
      case NOAUTH:
        dead = true;
        listener.closing(NOAUTH);
        return;
      default:
        // Retry errors
        zk.getChildren(znode.getPath(), this, this, null);
        return;
    }

    List<String> n = null;
    if (!exists) {
      return;
    }
    if ((children == null && children != prevData)
        || (children != null && !children.equals(prevData))) {
      listener.exists(children);
      prevData = children;
    }
  }

  /**
   * Other classes use the DataMonitor by implementing this method
   */
  public interface ChildrenMonitorListener {

    /**
     * The existence status of the node has changed.
     *
     * @param children
     */
    void exists(List<String> children);

    /**
     * The ZooKeeper session is no longer valid.
     *
     * @param code
     */
    void closing(Code code);

    /**
     * The data on the node has changed.
     *
     * @param children
     */
    void dataChanged(List<String> children);
  }

  @Override
  public void process(WatchedEvent event) {
    String path = event.getPath();
    if (event.getType() == Event.EventType.None) {
      // We are are being told that the state of the
      // connection has changed
      switch (event.getState()) {
        case SyncConnected:
          // In this particular example we don't need to do anything
          // here - watches are automatically re-registered with
          // server and any watches triggered while the client was
          // disconnected will be delivered (in order of course)
          break;
        case Expired:
          // It's all over
          dead = true;
          listener.closing(KeeperException.Code.SESSIONEXPIRED);
          break;
      }
    } else if (path != null && path.equals(znode.getPath())) {
      if (event.getType() == Event.EventType.NodeChildrenChanged) {
        try {
          List<String> n = zk.getChildren(znode.getPath(), false, null);
          listener.dataChanged(n);
        } catch (KeeperException e) {
          // We don't need to worry about recovering now. The watch
          // callbacks will kick off any exception handling
          LOG.error("DataMonitor KeeperException", e);
        } catch (InterruptedException e) {
          return;
        }
      } else if (event.getType() == Event.EventType.NodeDeleted) {
        dead = true;
        listener.closing(KeeperException.Code.NONODE);
      }
      // Something has changed on the node, let's find out
      zk.getChildren(znode.getPath(), this, this, null);
    }
    if (chainedWatcher != null) {
      chainedWatcher.process(event);
    }
  }

  @Override
  public boolean isDead() {
    return dead;
  }

}
