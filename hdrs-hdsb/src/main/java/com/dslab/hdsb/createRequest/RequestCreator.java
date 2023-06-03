/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.createRequest;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * @author Weli
 */
public class RequestCreator {

  private BlockingQueue<URL> bq;

  public RequestCreator(String access, Iterator<String> from, Iterator<String> to, int requestNum) throws MalformedURLException {
    this.bq = new ArrayBlockingQueue<>(requestNum);
    for (int i = 0; i < requestNum; i++) {
      StringBuilder req = new StringBuilder();
      req.append(access)
              .append("?from=")
              .append(encode(from.next()))
              .append("&to=")
              .append(encode(to.next()));
      bq.add(new URL(req.toString()));
    }
  }

  public BlockingQueue<URL> getReqQueue() {
    return this.bq;
  }

  public int getSize() {
    return this.bq.size();
  }

  private static String encode(String url) {
    try {
      return URLEncoder.encode(url, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }
}
