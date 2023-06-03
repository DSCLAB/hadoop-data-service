/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.response;

import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author Weli
 */
public class Response {

  private final BlockingQueue<ResponseItems> response;
  private final BlockingQueue<ResponseItems> scheduler;

  public Response(int requestNum) {

    response = new ArrayBlockingQueue<>(requestNum);
    scheduler = new ArrayBlockingQueue<>(requestNum);
  }

  public void setResponseQueue(ResponseItems e) {
    response.add(e);
  }

  public BlockingQueue<ResponseItems> getResponseQueue() {
    return this.response;
  }

  public void setSchedulerQueue(ResponseItems e) {
    scheduler.add(e);
  }

  public BlockingQueue<ResponseItems> getSchedulerQueue() {
    return this.scheduler;
  }

}
