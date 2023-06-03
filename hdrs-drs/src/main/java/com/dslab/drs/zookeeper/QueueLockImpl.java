package com.dslab.drs.zookeeper;

import com.dslab.drs.exception.DrsQueueException;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class QueueLockImpl extends ZookeeperAccesor implements QueueLock {

  private static final Log LOG = LogFactory.getLog(QueueLockImpl.class);

  final String dir;
  final String waitDir;
  final byte[] data;
  final String appId;
  final String[] queues;
  final int maxWaitSize;

  private String waitId;
  private String queueId;
  private String ownerId;
  private ZNodeName idName;
  private String queueName;
  private String errorMessage;

  private CountDownLatch latch;
  private SortedSet<ZNodeName> lessThanMe;

  /**
   * 兩階段鎖， 第一階段是 waitId 放置於 /drs/blocked 第二階段是 queueId 放置於 /drs
   */
  public QueueLockImpl(DrsConfiguration conf, String appId, String[] queues, int waitSize) throws IOException {
    super(conf);
    this.dir = DrsConfiguration.ZK_DRS_ROOT_DIR;
    this.waitDir = dir + "/" + DrsConfiguration.ZK_DRS_WAIT_DIR_NAME;
    this.data = (appId == null) ? null : appId.getBytes();
    this.appId = appId;
    this.queues = queues;
    this.maxWaitSize = waitSize;
  }

  @Override
  public String waitUntilRunningOnQueues() throws DrsQueueException {
    if (isClosed()) {
      return null;
    }

    ensurePathExists(dir);
    ensurePathExists(waitDir);

    LOG.info("1.WatiAccessQueueOperation");
    try {
      retryOperation(new WatiAccessQueueOperation());
    } catch (KeeperException | InterruptedException ex) {
      Logger.getLogger(QueueLockImpl.class.getName()).log(Level.SEVERE, null, ex);
    }

    LOG.info("2.LockQueueOperation ");
    try {
      retryOperation(new LockQueueOperation(queueName));
    } catch (KeeperException | InterruptedException ex) {
      Logger.getLogger(QueueLockImpl.class.getName()).log(Level.SEVERE, null, ex);
    }

    LOG.info("3:UnlockOperation waitId.");

    try {
      retryOperation(new UnlockOperation(waitId));
    } catch (KeeperException | InterruptedException ex) {
      Logger.getLogger(QueueLockImpl.class.getName()).log(Level.SEVERE, null, ex);
    }

    if (errorMessage != null) {
      throw new DrsQueueException(errorMessage);
    }

    return queueName;
  }

  @Override
  public void unlockQueue() throws KeeperException, InterruptedException {
    retryOperation(new UnlockOperation(queueId));
  }

  private String getFreeQueueName() throws KeeperException, InterruptedException {
    List<String> runningQueues = new ArrayList<>();
    List<String> childrens = zookeeper.getChildren(dir, false);
    List<String> queueList = Arrays.asList(queues);

    for (String children : childrens) {
      String[] queueSplit = children.split("-");
      String queueName = queueSplit[0];
      if (queueSplit.length > 3) {
        for (int i = 1; i < queueSplit.length - 2; i++) {
          queueName = queueName + "-" + queueSplit[i];
        }
      }
      if (!DrsConfiguration.ZK_DRS_WAIT_DIR_NAME.equals(queueName)
              && queueList.contains(queueName)) {
        runningQueues.add(queueName);
        LOG.debug("add queueName:" + queueName);
      }
    }

    for (String queue : queueList) {
      if (!runningQueues.contains(queue)) {
        LOG.info(queue + " is not running.");
        return queue;
      }
    }

    LOG.info("No queue can running");
    return null;
  }

  private class WatiAccessQueueOperation implements ZooKeeperOperation<String> {

    private void createWaitNode(String prefix, ZooKeeper zookeeper, String dir)
            throws KeeperException, InterruptedException {
      List<String> names = zookeeper.getChildren(dir, false);
      for (String name : names) {
        if (name.startsWith(prefix)) {
          waitId = dir + "/" + name;
        }
        break;
      }
      if (waitId == null) {
        waitId = zookeeper.create(dir + "/" + prefix, data, getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
        LOG.info("create wait id = " + waitId);
      }
    }

    @Override
    public String execute() throws KeeperException, InterruptedException {

      if (waitId == null) {
        long sessionId = zookeeper.getSessionId();
        String prefix = appId + "-" + sessionId + "-";
        createWaitNode(prefix, zookeeper, waitDir);
        idName = new ZNodeName(waitId);
      }

      if (waitId != null) {

        LOG.info("watch self id :" + waitId);
        Stat waitIdStat = zookeeper.exists(waitId, new SelfWatcher());
        if (waitIdStat == null) {
          LOG.info("SelfWatcher get null state. id:" + waitId);
        }

        List<String> names = zookeeper.getChildren(waitDir, false);
        if (names.isEmpty()) {
          waitId = null;
        } else {
          SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
          for (String name : names) {
            sortedNames.add(new ZNodeName(waitDir + "/" + name));
          }
          ownerId = sortedNames.first().getName();
          lessThanMe = sortedNames.headSet(idName);

          if (!lessThanMe.isEmpty()) {
            LOG.info("lessThan size : " + lessThanMe.size());
            if (lessThanMe.size() >= maxWaitSize) {
              errorMessage = "Drs queue is full,max " + maxWaitSize;
              return null;
            }
            latch = new CountDownLatch(lessThanMe.size());
            for (ZNodeName child : lessThanMe) {
              String childId = child.getName();
              Stat childStat = zookeeper.exists(childId, new BlockedQueueWatcher());
              LOG.info(childId + " less than me.");
              if (childStat == null) {
                LOG.info(childId + " get null state.");
              }
            }

            LOG.info("Wait " + waitId + " get lock");
            latch.await();
          }

          LOG.info(waitId + " get lock");
          while (queueName == null && !isClosed()) {
            LOG.info("idName " + idName + " wating.");
            queueName = getFreeQueueName();
            TimeUnit.SECONDS.sleep(1);
          }
          LOG.info("get queueName " + queueName);
          return queueName;
        }
      }
      return null;
    }

  }

  private class LockQueueOperation implements ZooKeeperOperation<Boolean> {

    private final String queueName;

    public LockQueueOperation(String queueName) {
      this.queueName = queueName;
    }

    private void createQueueNode(String prefix, ZooKeeper zookeeper, String dir)
            throws KeeperException, InterruptedException {
      queueId = zookeeper.create(dir + "/" + prefix, data, getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
      LOG.info("create queueId = " + queueId);
    }

    @Override
    public Boolean execute() throws KeeperException, InterruptedException {
      if (queueName != null) {
        long sessionId = zookeeper.getSessionId();
        String prefix = queueName + "-" + sessionId + "-";
        createQueueNode(prefix, zookeeper, dir);
        return true;
      } else {
        return false;
      }
    }

  }

  private class UnlockOperation implements ZooKeeperOperation<Boolean> {

    final String unlockID;

    public UnlockOperation(String unlockID) {
      this.unlockID = unlockID;
    }

    @Override
    public Boolean execute() throws KeeperException, InterruptedException {
      LOG.info("unlock:" + unlockID);
      if (unlockID != null) {
        zookeeper.delete(unlockID, -1);
      } else {
        LOG.info("UnlockWaitOperation's id is null.");
      }
      return true;
    }
  }

  private class GetDataOperation implements ZooKeeperOperation<byte[]> {

    private final String path;

    public GetDataOperation(String path) {
      this.path = path;
    }

    @Override
    public byte[] execute() throws KeeperException, InterruptedException {
      return zookeeper.getData(path, false, new Stat());
    }

  }

  public boolean isOwner() {
    return waitId != null && ownerId != null && waitId.equals(ownerId);
  }

  private class BlockedQueueWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      LOG.info("event(blocked queue):" + event.toString());
      latch.countDown();
      LOG.info("latch count:" + latch.getCount());
    }
  }

  private class SelfWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      LOG.info("event(self):" + event.toString());

      if (EventType.NodeDeleted == event.getType()) {
        LOG.info("event(self): get EventType.NodeDeleted ,delete ID : " + waitId);
        waitId = null;
        close();
        if (latch != null) {
          while (latch.getCount() != 0) {
            latch.countDown();
          }
        }
        //waitId 的 znode 不該在取得queueName前被刪除
        if(queueName==null){
          errorMessage = "The waiting znode be killed.";
        }
      }
    }
  }

}
