/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.response;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class ResponseMonitor implements Runnable {

  private static final Log LOG
          = LogFactory.getLog(ResponseMonitor.class);
  private final Response res;
  private final CountDownLatch schedulerlatch;
  private final List<String> csvResults;
  private final List<String> errors;
  private final List<Integer> throughputs;
  private final List<Integer> latencys;
  private final List<String> serverNames;
  private final List<Double> throughputToDoubles;
  private final String logOutputPath;
  private final long startTime = System.currentTimeMillis();
  private static final char COLON = '"';
  private static final String COMMA = ",";
  private final int reqNum;
  Server umc01;
  Server umc02;
  Server umc03;
  Server umc04;
  Server umc05;
  Server umc06;
  Server umc07;
  Server umc08;
  Server umc09;
  Server umc10;
  Server umc11;
  Server umc12;
  Server umc13;
  Server umc14;
  Server umc15;

  public ResponseMonitor(Response res, CountDownLatch schedulerlatch, int reqNum, String logOutputPath) {
    this.res = res;
    this.schedulerlatch = schedulerlatch;
    csvResults = new ArrayList<>(reqNum);
    errors = new ArrayList<>(reqNum);
    throughputs = new ArrayList<>(reqNum);
    latencys = new ArrayList<>(reqNum);
    serverNames = new ArrayList<>(reqNum);
    throughputToDoubles = new ArrayList<>(reqNum);
    if (!logOutputPath.endsWith("/")) {
      logOutputPath = logOutputPath + "/";
    }
    this.logOutputPath = logOutputPath;
    this.reqNum = reqNum;
    umc01 = new Server("umc-01", reqNum);
    umc02 = new Server("umc-02", reqNum);
    umc03 = new Server("umc-03", reqNum);
    umc04 = new Server("umc-04", reqNum);
    umc05 = new Server("umc-05", reqNum);
    umc06 = new Server("umc-06", reqNum);
    umc07 = new Server("umc-07", reqNum);
    umc08 = new Server("umc-08", reqNum);
    umc09 = new Server("umc-09", reqNum);
    umc10 = new Server("umc-10", reqNum);
    umc11 = new Server("umc-11", reqNum);
    umc12 = new Server("umc-12", reqNum);
    umc13 = new Server("umc-13", reqNum);
    umc14 = new Server("umc-14", reqNum);
    umc15 = new Server("umc-15", reqNum);
  }

  private long getElapsed() {
    return System.currentTimeMillis() - startTime;
  }

  @Override
  public void run() {
    try {
      File dir = new File(logOutputPath);
      if (!dir.exists()) {
        dir.mkdir();
      }
      while (!Thread.interrupted()) {
        ResponseItems response = res.getResponseQueue().take();
        schedulerlatch.countDown();
        loadQueue(response.getLatency(), response.getThroughput(), response.getError(),
                response.getResponse(), response.getServerName(), response.getThroughputToDouble());
      }
    } catch (InterruptedException ex) {
    } finally {
      try {
        writeFile(csvResults, logOutputPath + "benchmark.csv");
        writeFile(errors, logOutputPath + "errLog");
        writeThroughput(throughputs, throughputToDoubles, serverNames);
        outputLatency(latencys, serverNames);
        outputThroughputWithServerName();
        outputLatencyWithServerName();
      } catch (IOException ex) {
        LOG.warn(ex);
      } finally {
        LOG.info("finish writting CSV and log");
      }
    }
  }

  private void loadQueue(int latency, int throughput, String err, String res,
          String serverName, double throughputPerSecond) {
    if (err.equals("")) {
      serverNames.add(serverName);
      csvResults.add(res);
      latencys.add(latency);
      throughputs.add(throughput);
      throughputToDoubles.add(throughputPerSecond);
    } else {
      errors.add(err);
    }
  }

  private void writeFile(List<String> resultQueue, String filePath) throws IOException {
    if (!resultQueue.isEmpty()) {
      try (FileWriter writer = new FileWriter(filePath)) {
        for (String s : resultQueue) {
          writer.write(s);
          writer.append("\n");
        }
      } catch (IOException ex) {
        LOG.warn(ex);
      }
    }
  }

  private void writeThroughput(List<Integer> fileSizes, List<Double> throughputPerSeconds, List<String> serverName) throws IOException {
    long elpased = getElapsed();
    double avg = fileSizes.stream().mapToDouble(v -> (double) v / (double) elpased).sum();
    LOG.info("Throughput = " + avg + " KB");
    int listSize = serverName.size();
    Iterator<String> serverNames = serverName.iterator();
    for (int i = 0; i < listSize; i++) {
      addThroughputWithServerName(serverNames.next().substring(14, 20).trim(), throughputPerSeconds.get(i));
    }
    writeFile(Collections.singletonList(String.valueOf(avg)), logOutputPath + "throughputLog");
  }

  private void outputLatency(List<Integer> latencys, List<String> serverName) throws IOException {
    if (!latencys.isEmpty()) {
      int latencyNum = latencys.size();
      int maxSize = latencys.stream().max(Comparator.comparingInt(v -> v)).get();
      int minSize = latencys.stream().min(Comparator.comparingInt(v -> v)).get();
      int avgSize = latencys.stream().reduce(0, (acc, e) -> acc + e) / latencyNum;
      StringBuilder latencyString = new StringBuilder();
      latencyString.append(COLON).append("latency").append(COLON).append(COMMA).append(COLON)
              .append(avgSize).append(COLON).append(COMMA).append(COLON).append("maxlatency").append(COLON)
              .append(COMMA).append(COLON).append(maxSize).append(COLON).append(COMMA)
              .append(COLON).append("minlatency").append(COLON).append(COMMA).append(COLON)
              .append(minSize).append(COLON);
      LOG.info("max_Latency = " + maxSize + " ms, min_Latency = " + minSize + " ms, avg_Latency = " + avgSize + " ms");
      List<String> lists = latencys.stream().map(String::valueOf).collect(Collectors.toList());
      List<String> list = new ArrayList<>(reqNum);
      int listSize = lists.size();
      Iterator<String> serverNames = serverName.iterator();
      Iterator<String> latencyToStrings = lists.iterator();
      for (int i = 0; i < listSize; i++) {
        String s = serverNames.next();
        list.add(s + COMMA + latencyToStrings.next());
        addLatencyWithServerName(s.substring(14, 20).trim(), latencys.get(i));
      }
      writeFile(list, logOutputPath + "latency.csv");
    }
  }

  private void addLatencyWithServerName(String name, int latency) {
    switch (name) {
      case "umc-01":
        umc01.addLatency(latency);
        break;
      case "umc-02":
        umc02.addLatency(latency);
        break;
      case "umc-03":
        umc03.addLatency(latency);
        break;
      case "umc-04":
        umc04.addLatency(latency);
        break;
      case "umc-05":
        umc05.addLatency(latency);
        break;
      case "umc-06":
        umc06.addLatency(latency);
        break;
      case "umc-07":
        umc07.addLatency(latency);
        break;
      case "umc-08":
        umc08.addLatency(latency);
        break;
      case "umc-09":
        umc09.addLatency(latency);
        break;
      case "umc-10":
        umc10.addLatency(latency);
        break;
      case "umc-11":
        umc11.addLatency(latency);
        break;
      case "umc-12":
        umc12.addLatency(latency);
        break;
      case "umc-13":
        umc13.addLatency(latency);
        break;
      case "umc-14":
        umc14.addLatency(latency);
        break;
      case "umc-15":
        umc15.addLatency(latency);
        break;
      default:
        break;
    }
  }

  private void addThroughputWithServerName(String name, double throughput) {
    switch (name) {
      case "umc-01":
        umc01.addThroughput(throughput);
        break;
      case "umc-02":
        umc02.addThroughput(throughput);
        break;
      case "umc-03":
        umc03.addThroughput(throughput);
        break;
      case "umc-04":
        umc04.addThroughput(throughput);
        break;
      case "umc-05":
        umc05.addThroughput(throughput);
        break;
      case "umc-06":
        umc06.addThroughput(throughput);
        break;
      case "umc-07":
        umc07.addThroughput(throughput);
        break;
      case "umc-08":
        umc08.addThroughput(throughput);
        break;
      case "umc-09":
        umc09.addThroughput(throughput);
        break;
      case "umc-10":
        umc10.addThroughput(throughput);
        break;
      case "umc-11":
        umc11.addThroughput(throughput);
        break;
      case "umc-12":
        umc12.addThroughput(throughput);
        break;
      case "umc-13":
        umc13.addThroughput(throughput);
        break;
      case "umc-14":
        umc14.addThroughput(throughput);
        break;
      case "umc-15":
        umc15.addThroughput(throughput);
        break;
      default:
        break;
    }
  }

  private void outputThroughputWithServerName() throws IOException {
    List<String> list = new ArrayList<>(16);
    list.add(umc01.getName() + COMMA + String.valueOf((umc01.sumThroughput() * 1000) / (umc01.sumLatency() * 1024)));
    list.add(umc02.getName() + COMMA + String.valueOf((umc02.sumThroughput() * 1000) / (umc02.sumLatency() * 1024)));
    list.add(umc03.getName() + COMMA + String.valueOf((umc03.sumThroughput() * 1000) / (umc03.sumLatency() * 1024)));
    list.add(umc04.getName() + COMMA + String.valueOf((umc04.sumThroughput() * 1000) / (umc04.sumLatency() * 1024)));
    list.add(umc05.getName() + COMMA + String.valueOf((umc05.sumThroughput() * 1000) / (umc05.sumLatency() * 1024)));
    list.add(umc06.getName() + COMMA + String.valueOf((umc06.sumThroughput() * 1000) / (umc06.sumLatency() * 1024)));
    list.add(umc07.getName() + COMMA + String.valueOf((umc07.sumThroughput() * 1000) / (umc07.sumLatency() * 1024)));
    list.add(umc08.getName() + COMMA + String.valueOf((umc08.sumThroughput() * 1000) / (umc08.sumLatency() * 1024)));
    list.add(umc09.getName() + COMMA + String.valueOf((umc09.sumThroughput() * 1000) / (umc09.sumLatency() * 1024)));
    list.add(umc10.getName() + COMMA + String.valueOf((umc10.sumThroughput() * 1000) / (umc10.sumLatency() * 1024)));
    list.add(umc11.getName() + COMMA + String.valueOf((umc11.sumThroughput() * 1000) / (umc11.sumLatency() * 1024)));
    list.add(umc12.getName() + COMMA + String.valueOf((umc12.sumThroughput() * 1000) / (umc12.sumLatency() * 1024)));
    list.add(umc13.getName() + COMMA + String.valueOf((umc13.sumThroughput() * 1000) / (umc13.sumLatency() * 1024)));
    list.add(umc14.getName() + COMMA + String.valueOf((umc14.sumThroughput() * 1000) / (umc14.sumLatency() * 1024)));
    list.add(umc15.getName() + COMMA + String.valueOf((umc15.sumThroughput() * 1000) / (umc15.sumLatency() * 1024)));
    writeFile(list.stream().map(String::valueOf).collect(Collectors.toList()),
            logOutputPath + "throughputWithServer");
  }

  private void outputLatencyWithServerName() throws IOException {
    List<String> list = new ArrayList<>(reqNum);
    list.add(umc01.getName() + COMMA + String.valueOf(umc01.sumLatency()));
    list.add(umc02.getName() + COMMA + String.valueOf(umc02.sumLatency()));
    list.add(umc03.getName() + COMMA + String.valueOf(umc03.sumLatency()));
    list.add(umc04.getName() + COMMA + String.valueOf(umc04.sumLatency()));
    list.add(umc05.getName() + COMMA + String.valueOf(umc05.sumLatency()));
    list.add(umc06.getName() + COMMA + String.valueOf(umc06.sumLatency()));
    list.add(umc07.getName() + COMMA + String.valueOf(umc07.sumLatency()));
    list.add(umc08.getName() + COMMA + String.valueOf(umc08.sumLatency()));
    list.add(umc09.getName() + COMMA + String.valueOf(umc09.sumLatency()));
    list.add(umc10.getName() + COMMA + String.valueOf(umc10.sumLatency()));
    list.add(umc11.getName() + COMMA + String.valueOf(umc11.sumLatency()));
    list.add(umc12.getName() + COMMA + String.valueOf(umc12.sumLatency()));
    list.add(umc13.getName() + COMMA + String.valueOf(umc13.sumLatency()));
    list.add(umc14.getName() + COMMA + String.valueOf(umc14.sumLatency()));
    list.add(umc15.getName() + COMMA + String.valueOf(umc15.sumLatency()));
    writeFile(list.stream().map(String::valueOf).collect(Collectors.toList()),
            logOutputPath + "latencyWithServer");
  }

}
