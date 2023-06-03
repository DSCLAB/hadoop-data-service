/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.response;

/**
 *
 * @author Weli
 */
public class ResponseItems {

  private final int throughput;
  private final int latency;
  private final String err;
  private final String response;
  private final String serverName;
  private final double throughputToDouble;

  public ResponseItems(int throughput, int latency, String err, String response,
          String serverName, double throughputToDouble) {
    this.throughput = throughput;
    this.latency = latency;
    this.err = err;
    this.response = response;
    this.serverName = serverName;
    this.throughputToDouble = throughputToDouble;
  }

  public int getThroughput() {
    return this.throughput;
  }

  public String getError() {
    return this.err;
  }

  public int getLatency() {
    return this.latency;
  }

  public String getResponse() {
    return this.response;
  }

  public String getServerName() {
    return this.serverName;
  }

  public double getThroughputToDouble() {
    return this.throughputToDouble;
  }
}
