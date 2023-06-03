package com.dslab.drs.zookeeper;

import com.dslab.drs.exception.DrsQueueException;
import org.apache.zookeeper.KeeperException;

public interface ZooKeeperOperation<T> {
	public T execute() throws KeeperException, InterruptedException;
}
