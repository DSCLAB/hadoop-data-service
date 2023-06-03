package com.dslab.drs.zookeeper;

import com.dslab.drs.exception.DrsClientException;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

public class TestQueueLock {

  private static final Log LOG = LogFactory.getLog(TestQueueLock.class);

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();
//    conf.set("hbase.zookeeper.quorum","192.168.103.106:2181");
    conf.set(DrsConfiguration.ZOOKEEPER_QUORUM, "drs-00,drs-05,drs-01");
    conf.set(DrsConfiguration.ZOOKEEPER_SESSION_TIMEOUT, "18000000");

    String[] queues = {"queue-1", "queue-2"};
    QueueLock lock = new QueueLockImpl(conf, "application" + System.currentTimeMillis(), queues, 2);
    String queueName = null;
    try {
      queueName = lock.waitUntilRunningOnQueues();
    } catch (IOException ex) {
      LOG.info("Get exception when lock :" + ex.getMessage());
      throw new DrsClientException(ex.getMessage());
    }

    if (queueName == null) {
      throw new DrsClientException("This job encounter unexpected error when waiting for queue.");
    }
    System.out.println("main() get lock. do something in " + queueName);;
    System.out.println("wait 120 second");;
    TimeUnit.SECONDS.sleep(120);
    System.out.println("wait finished");;
    lock.unlockQueue();
    System.out.println("Stop Thread");
  }
}
