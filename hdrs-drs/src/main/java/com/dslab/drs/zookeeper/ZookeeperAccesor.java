package com.dslab.drs.zookeeper;

import com.dslab.drs.utils.DrsConfiguration;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZookeeperAccesor implements Closeable {

  private static final Log LOG = LogFactory.getLog(ZookeeperAccesor.class);

  protected ZooKeeper zookeeper;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final long retryDelay = 10L;
  private final int retryCount = 10;
  private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

  public ZookeeperAccesor(ZooKeeper zookeeper) {
    this.zookeeper = zookeeper;
  }

  public ZookeeperAccesor(Configuration conf) throws IOException {
    buildZKConn(conf);
  }

  private void buildZKConn(Configuration conf) throws IOException {
    zookeeper = new ZooKeeper(conf.get(DrsConfiguration.ZOOKEEPER_QUORUM),
            conf.getInt(DrsConfiguration.ZOOKEEPER_SESSION_TIMEOUT, DrsConfiguration.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT),
            new DefaultZKWatcher(conf));
  }

  public ZooKeeper getZookeeper() {
    return this.zookeeper;
  }

  public List<ACL> getAcl() {
    return this.acl;
  }

  public void setAcl(List<ACL> acl) {
    this.acl = acl;
  }

  public long getRetryDelay() {
    return this.retryDelay;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      doClose();
    }
  }

  protected void doClose() {
  }

  public void closeConnection() {
    try {
      zookeeper.close();
    } catch (Exception ex) {
      LOG.warn(ex);
    }
  }

  protected void ensurePathExists(String path) {
    ensureExists(path, null, acl, CreateMode.PERSISTENT);
  }

  protected void ensureExists(final String path, final byte[] data, final List<ACL> acl, final CreateMode flags) {
    try {
      retryOperation(new ZooKeeperOperation<Boolean>() {
        @Override
        public Boolean execute() throws KeeperException, InterruptedException {
          Stat stat = zookeeper.exists(path, false);
          if (stat != null) {
            return true;
          }
          zookeeper.create(path, data, acl, flags);
          return true;
        }
      });
    } catch (KeeperException | InterruptedException e) {
      LOG.error("ensureExists error : " + e.getMessage());
    }
  }

  protected Object retryOperation(ZooKeeperOperation operation) throws KeeperException, InterruptedException {
    KeeperException exception = null;
    for (int i = 0; i < retryCount; i++) {
      try {
        return operation.execute();
      } catch (KeeperException.SessionExpiredException e) {
        throw e;
      } catch (KeeperException.ConnectionLossException e) {
        if (exception == null) {
          exception = e;
        }
        retryDelay(i);
      }
    }
    return exception;
  }

  protected boolean isClosed() {
    return closed.get();
  }

  protected void retryDelay(int attemptCount) {
    if (attemptCount > 0) {
      try {
        TimeUnit.SECONDS.sleep(attemptCount * retryDelay);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private class DefaultZKWatcher implements Watcher {

    private final Configuration conf;

    public DefaultZKWatcher(Configuration conf) {
      this.conf = conf;
    }

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

}
