/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package umc.cdc.hds.lock;

import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.exceptions.LockException;
import umc.cdc.hds.tools.ThreadPoolManager;
import umc.cdc.hds.uri.UriRequest;
import umc.cdc.hds.zookeeper.ZKAccessor;
import umc.cdc.hds.zookeeper.ZNodePath;
import umc.udp.core.framework.ShareableObject;

public class HDSLockManager implements GlobalLockManager {

  private final ShareableObject<ZKAccessor> zookeeper;
  // ":" is a delimiter for sequence lock
  private static final String READ_FLAG = "read:";
  private static final String WRITE_FLAG = "write";
  private static final String LOCK_PATH_ORG = "/hds/lock";
  private static final String LOCK_PATH = LOCK_PATH_ORG + "/";
  private static final Log LOG = LogFactory.getLog(HDSLockManager.class);
  private static final String UNKNOWN_KEY = "unknown key";
  private final byte[] serverName;
  private final static AtomicInteger READ_LOCK_NUM = new AtomicInteger(0);
  private final static AtomicInteger WRITE_LOCK_NUM = new AtomicInteger(0);
  private final int createLockRetryMax;

  // very expensive method ! should be only used in debug mode.
  private long getLockNum(READ_OR_WRITE category) {
    long lockAmount = 0;
    try {
      ZNodePath zp = new ZNodePath(LOCK_PATH_ORG);
      List<String> readLocks = zookeeper.get().getChildren(zp);
      StringBuilder sb = new StringBuilder();
      for (String rl : readLocks) {
        sb.append(zp.getPath()).append("/").append(rl);
        // znode path contains charsequence with read/write which
        // is lowercase.
        for (String children : zookeeper.get().getChildren(new ZNodePath(sb.toString()))) {
          if (children.startsWith(category.name().toLowerCase())) {
            lockAmount++;
          }
        }
        sb.setLength(0);
      }
    } catch (KeeperException | InterruptedException ex) {
      LOG.error(ex);
    }
    return lockAmount;
  }

  @Override
  public long getReadLockNum() {
    return READ_LOCK_NUM.get();
  }

  @Override
  public long getWriteLockNum() {
    return WRITE_LOCK_NUM.get();
  }

  private static enum READ_OR_WRITE {
    READ,
    WRITE
  };

  public HDSLockManager(Configuration conf) {
    try {
      createLockRetryMax = conf.getInt(HDSConstants.CREATE_LOCK_MAX_RETRTY,
          HDSConstants.DEFAULT_CREATE_LOCK_MAX_RETRTY);
      zookeeper = ZKAccessor.getInstance(conf);
      serverName = Bytes.toBytes(ServerName.valueOf(
          InetAddress.getLocalHost().getHostName(),
          conf.getInt(HDSConstants.HTTP_SERVER_PORT,
              HDSConstants.DEFAULT_HTTP_SERVER_PORT), 0).toString());
      ThreadPoolManager.scheduleWithFixedDelay(() -> {
        try {
          zookeeper.get().clearChildern(new ZNodePath("/hds/lock"), serverName);
        } catch (KeeperException | InterruptedException ex) {
          LOG.error(ex);
        }
      }, conf.getLong(HDSConstants.LOCK_NODE_TIME_TO_CLEAN,
          HDSConstants.DEFAULT_LOCK_NODE_TIME_TO_CLEAN), TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      throw new LockException(ex);
    }
  }

  private static String generalKey(UriRequest uri) throws IOException {
    try {
      return URLEncoder.encode(uri.toRealUri()
              + uri.getQuery().getQueryValue(HDSConstants.HDS_URI_ARG_VERSION, HDSConstants.HDS_VERSION)
              + HDSConstants.DEFAULT_ROWKEY_SEPARATOR,
              HDSConstants.DEFAULT_CHAR_ENCODING);
    } catch (UnsupportedEncodingException ex) {
      throw new LockException(ex);
    }
  }

  private static ZNodePath createFileNodePath(UriRequest uri) throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append(LOCK_PATH);
    builder.append(generalKey(uri));
    return new ZNodePath(builder.toString());
  }

  private static ZNodePath createLockNodePath(ZNodePath filePath, READ_OR_WRITE readwrite) {
    StringBuilder builder = new StringBuilder();
    builder.append(filePath.getPath()).append("/");
    switch (readwrite) {
      case READ:
        builder.append(READ_FLAG);
        break;
      case WRITE:
        builder.append(WRITE_FLAG);
        break;
      default:
        builder.append(WRITE_FLAG);
        //never happened
    }
    return new ZNodePath(builder.toString());
  }

  private boolean checkReadLockHolding(ZNodePath filePath)
      throws KeeperException, InterruptedException {
    ZKAccessor zk = zookeeper.get();
    try {
      List<String> children = zk.getChildren(filePath);
      if (!children.isEmpty()) {
        if (children.get(0).startsWith(WRITE_FLAG)) {
          return true;
        }
      }
    } catch (KeeperException ex) {
      if (ex.code().intValue() == Code.NONODE.intValue()) {
        return false;
      } else {
        throw ex;
      }
    }

    return false;
  }

  private boolean checkWriteLockHolding(ZNodePath filePath)
      throws KeeperException, InterruptedException {
    ZKAccessor zk = zookeeper.get();
    try {
      if (!zk.getChildren(filePath).isEmpty()) {
        return true;
      }
    } catch (KeeperException ex) {
      if (ex.code().intValue() == Code.NONODE.intValue()) {
        return false;
      } else {
        throw ex;
      }
    }
    return false;
  }

  private void deleteLockNode(final ZNodePath lockPath, final ZNodePath filePath)
      throws InterruptedException, KeeperException {
    retryDeleteChildrenLockNode(lockPath);
    retryDeleteParentLockNode(filePath);
  }

  private void retryDeleteChildrenLockNode(final ZNodePath path)
      throws InterruptedException, KeeperException {
    ZKAccessor zk = zookeeper.get();
    int retry = 0;
    int maxRetry = 10;
    while (true) {
      try {
        if (zk.isNodeExist(path)) {
          zk.deleteNode(path);
        }
        break;
      }
      catch (Exception ex) {
        if (ex instanceof NoNodeException) {
          return;
        }
        LOG.error("retry delete children lock.", ex);
        if (++retry == maxRetry) {
          LOG.error("retry delete children lock fail.", ex);
          throw ex;
        }
      }
    }
  }

  private void retryDeleteParentLockNode(final ZNodePath path)
      throws InterruptedException, KeeperException {
    ZKAccessor zk = zookeeper.get();
    int retry = 0;
    int maxRetry = 10;
    while (true) {
      try {
        if (zk.isNodeExist(path) && zk.getChildren(path).isEmpty()) {
          zk.deleteNode(path);
        }
        break;
      } catch (Exception ex) {
        if (ex instanceof NoNodeException || ex instanceof NotEmptyException) {
          return;
        }
        LOG.error("retry delete parent lock.", ex);
        if (++retry == maxRetry) {
          LOG.error("retry delete parent lock fail.", ex);
          throw ex;
        }
      }
    }
  }

  @Override
  public Optional<Lock> getReadLock(UriRequest uri) throws IOException {
    if (uri.getProtocol().equals(Protocol.local)) {
      return emptyLock(uri);
    }
    ZNodePath filePath = createFileNodePath(uri);
    try {
      /**
       * There is a unknow stage when lock node has no children, this means that
       * the node is ready to be deleted, but it hasn't finished yet. But
       * checkReadLockHolding won't detected that. Then the KeeperException
       * NONODE will happen in createSequenceNode since the lock node was just
       * deleted.
       *
       * Now I add a retry here. But if another node do the same thing.
       * NODEEXIST may happen. This seems that the whole structure has some
       * trouble. We should redesign it.
       */
      Optional<String> readLockZnodePath = retryCreateReadLock(filePath, createLockRetryMax);
      if (!readLockZnodePath.isPresent()) {
        return Optional.empty();
      } else {
        ZNodePath realLockPath = new ZNodePath(readLockZnodePath.get());
        return Optional.of(new Lock() {
          {
            READ_LOCK_NUM.getAndIncrement();
          }

          @Override
          public byte[] getKey() {
            try {
              return Bytes.toBytes(generalKey(uri));
            } catch (IOException ex) {
              return Bytes.toBytes(UNKNOWN_KEY);
            }
          }

          @Override
          protected void internalClose() throws IOException {
            try {
              READ_LOCK_NUM.getAndDecrement();
              deleteLockNode(realLockPath, filePath);
            } catch (InterruptedException | KeeperException ex) {
              LOG.error("HDS file Lock Close error.", ex);
            }
          }

        });
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new LockException(ex);
    }
  }

  @Override
  public Optional<Lock> getWriteLock(UriRequest uri) throws IOException {
    if (uri.getProtocol().equals(Protocol.local)) {
      return emptyLock(uri);
    }
    ZNodePath filePath = createFileNodePath(uri);
    try {
      if (!retryCreateWriteLock(filePath, createLockRetryMax)) {
        return Optional.empty();
      } else {
        ZNodePath lockPath = createLockNodePath(filePath, READ_OR_WRITE.WRITE);
        return Optional.of(new Lock() {
          {
            WRITE_LOCK_NUM.getAndIncrement();
          }

          @Override
          public byte[] getKey() {
            try {
              return Bytes.toBytes(generalKey(uri));
            } catch (IOException ex) {
              return Bytes.toBytes(UNKNOWN_KEY);
            }
          }

          @Override
          protected void internalClose() throws IOException {
            try {
              WRITE_LOCK_NUM.getAndDecrement();
              deleteLockNode(lockPath, filePath);
            } catch (InterruptedException | KeeperException ex) {
              LOG.error("HDS file Lock Close error.", ex);
            }
          }

        });
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new LockException(ex);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      zookeeper.close();
    } catch (Exception ex) {
      throw new LockException(ex);
    }
  }

  private Optional<String> retryCreateReadLock(ZNodePath filePath, int maxRetry)
      throws InterruptedException, KeeperException {
    int retry = 0;

    while (true) {
      try {
        if (checkReadLockHolding(filePath)) {
          return Optional.empty();
        } else {
          ZNodePath lockPath = createLockNodePath(filePath, READ_OR_WRITE.READ);
          return createReadLock(lockPath);
        }
      } catch (InterruptedException | KeeperException ex) {
        if (++retry == maxRetry) {
          throw ex;
        }
      }
    }
  }

  private Optional<String> createReadLock(ZNodePath lockPath)
      throws InterruptedException, KeeperException {
    try {
      return Optional.of(
          zookeeper.get().createSequenceNode(lockPath, serverName));
    } catch (KeeperException ex) {
      if (ex.code().intValue() == Code.NODEEXISTS.intValue()) {
        return Optional.empty();
      } else {
        throw ex;
      }
    }
  }

  private boolean retryCreateWriteLock(ZNodePath filePath, int maxRetry)
      throws InterruptedException, KeeperException {
    int retry = 0;

    while (true) {
      try {
        if (checkWriteLockHolding(filePath)) {
          return false;
        } else {
          ZNodePath lockPath = createLockNodePath(filePath, READ_OR_WRITE.WRITE);
          return createWriteLock(lockPath);
        }
      } catch (InterruptedException | KeeperException ex) {
        if (++retry == maxRetry) {
          throw ex;
        }
      }
    }
  }

  private boolean createWriteLock(ZNodePath lockPath)
      throws InterruptedException, KeeperException {
    try {
      zookeeper.get().createNode(lockPath, serverName);
    } catch (KeeperException ex) {
      if (ex.code().intValue() == Code.NODEEXISTS.intValue()) {
        return false;
      } else {
        throw ex;
      }
    }
    return true;
  }

  private Optional<Lock> emptyLock(UriRequest uri) {
    return Optional.of(new Lock() {
      @Override
      public byte[] getKey() {
        try {
          return Bytes.toBytes(generalKey(uri));
        } catch (IOException ex) {
          return Bytes.toBytes("unknown key");
        }
      }

      @Override
      protected void internalClose() throws IOException {
        //do nothing
      }
    });
  }

}
