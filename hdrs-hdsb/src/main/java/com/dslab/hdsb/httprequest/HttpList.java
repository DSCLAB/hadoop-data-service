/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.httprequest;

import com.dslab.hdsb.core.ListStaging;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Weli
 */
public class HttpList {

  URL url;
  private final ListStaging assignSourceQueue;

  public HttpList(String req, ListStaging list) throws MalformedURLException {
    this.url = new URL(req);
    this.assignSourceQueue = list;
  }

  public void runListAPI() throws ProtocolException, IOException, InterruptedException {
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(3600000);
      conn.setReadTimeout(3600000);
      conn.setRequestMethod("GET");
      conn.connect();
      int responseCode = conn.getResponseCode();
      if (!(responseCode == 200)) {
        errorMessage(conn);
      } else {
        String res = getResponseMessage(conn);
        parseResponse(res);
      }
    } finally {
      if (!(conn == null)) {
        conn.disconnect();
      }
    }
  }

  private void parseResponse(String res) {
    while (res.contains("\"uri\":")) {
      int begin = res.indexOf("\"uri\":");
      int end = res.indexOf("\"location\":");
      String filePath = res.substring(begin + 6, end);
      String temp = res.substring(end + 10);
      res = temp;
      String file1 = filePath.replaceAll("\"", "");
      String file2 = file1.replaceAll(",", "");
      assignSourceQueue.setList(file2);
    }
  }

  private String getResponseMessage(HttpURLConnection conn) throws IOException, InterruptedException {
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      return response.toString();
    }
  }

  private void errorMessage(HttpURLConnection conn) throws IOException, InterruptedException {
    try (BufferedReader errin = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
      String errinputLine;
      StringBuilder errresponse = new StringBuilder();

      while ((errinputLine = errin.readLine()) != null) {
        errresponse.append(errinputLine);
      }
      errin.close();
      System.out.println(errresponse.toString());
    }
  }
}
