/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.response;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Weli
 */
public class Server {

  private final String name;
  private final List<Integer> latency;
  private final List<Double> throughput;

  protected Server(String name, int reqNum) {
    this.latency = new ArrayList<>(reqNum);
    this.throughput = new ArrayList<>(reqNum);
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public void addLatency(int l) {
    latency.add(l);
  }

  public int sumLatency() {
    return latency.stream().mapToInt(v -> v).sum();
  }

  public void addThroughput(double t) {
    throughput.add(t);
  }

  public double sumThroughput() {
    return throughput.stream().mapToDouble(v -> v).sum();
  }

}
