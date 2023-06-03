/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.core;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author Weli
 */
public class QueueStaging {

  private final BlockingQueue<String> BlockingQueue;

  public QueueStaging(int upperBound) {
    this.BlockingQueue = new ArrayBlockingQueue<>(upperBound);
  }

  public BlockingQueue<String> getIterable() {
    return BlockingQueue;
  }

  public void setQueue(String e) {
    BlockingQueue.add(e);
  }

  public int getSize() {
    return BlockingQueue.size();
  }

  public Boolean isEmpty() {
    return BlockingQueue.isEmpty();
  }

}
