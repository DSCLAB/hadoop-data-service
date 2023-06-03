/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.response;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class ResponseScheduler implements Runnable {

  private static final Log LOG
          = LogFactory.getLog(ResponseScheduler.class);

  private final Response res;
  private final String logOutputPath;
  private static int throughput;

  public ResponseScheduler(Response res, int reqNum, String logOutputPath) {
    this.res = res;
    this.logOutputPath = logOutputPath;
  }

  @Override
  public void run() {
    File dir = new File(logOutputPath);
    if (!dir.exists()) {
      dir.mkdir();
    }
    try {
      throughput = 0;
      int i = (res.getSchedulerQueue().size());
      while (i > 0) {
        ResponseItems response = res.getSchedulerQueue().take();
        throughput += response.getThroughput() / 1024;
        i--;
      }
    } catch (InterruptedException ex) {
    } finally {
      try {
        output();
      } catch (IOException ex) {
        LOG.warn(ex);
      }
    }
  }

  private void output() throws IOException {
    LOG.info("throughput per five seconds = " + (throughput));
    try (FileWriter writer = new FileWriter(logOutputPath + "/throughputPerFiveSecond", true)) {
      writer.write("throughput" + "," + (throughput));
      writer.append("\n");
    }
  }
}
