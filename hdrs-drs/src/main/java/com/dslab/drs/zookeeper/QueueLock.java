package com.dslab.drs.zookeeper;

import com.dslab.drs.exception.DrsQueueException;
import org.apache.zookeeper.KeeperException;

public interface QueueLock {

  public String waitUntilRunningOnQueues() throws DrsQueueException ;
  public void unlockQueue() throws   KeeperException, InterruptedException,DrsQueueException;
  public void closeConnection();
}
