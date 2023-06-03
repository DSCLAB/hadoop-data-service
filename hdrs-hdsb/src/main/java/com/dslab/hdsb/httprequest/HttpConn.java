/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.httprequest;

import com.dslab.hdsb.response.Response;
import com.dslab.hdsb.response.ResponseItems;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class HttpConn implements Runnable {

  //queue for store all requests to HttpConn
  private static final Log LOG
          = LogFactory.getLog(HttpConn.class);

  private final BlockingQueue<URL> requests;
  private final Iterator<Integer> frequencys;
  private final int reqNumPerThread;
  private final CountDownLatch latch;
  private final Response res;
  private static final char COLON = '"';
  private static final String COMMA = ",";
  private final int socketTimeOut;
  private long latency = 0;

  public HttpConn(BlockingQueue<URL> reqBlockingQueue, Iterator<Integer> frequencyBlockingQueue,
          int reqNumPerThread, CountDownLatch latch, Response res, int socketTimeOut) throws InterruptedException {
    this.requests = reqBlockingQueue;
    this.frequencys = frequencyBlockingQueue;
    this.reqNumPerThread = reqNumPerThread;
    this.latch = latch;
    this.res = res;
    this.socketTimeOut = socketTimeOut;
  }

  @Override
  public void run() {
    for (int i = 0; i < reqNumPerThread; i++) {
      if (latch.getCount() > 0 && !requests.isEmpty()) {
        try {
          innerRun();
        } catch (IOException ex) {
          LOG.warn(ex);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        } catch (URISyntaxException ex) {
          LOG.warn(ex.getStackTrace());
        }
      }
    }
  }

  protected void innerRun() throws IOException, InterruptedException, URISyntaxException {
    interruptThreadSleep();
    URL url = requests.take();
    Map<String, String> map = getQueryMap(url.getQuery());
    URI URLProtocol = new URI(URLDecoder.decode(map.get("from"), "UTF-8"));
    long httpStartTime = 0;
    if (!URLProtocol.getScheme().equals("local")) {
      httpStartTime = System.currentTimeMillis();
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(socketTimeOut);
        conn.setReadTimeout(socketTimeOut);
        conn.setRequestMethod("GET");
        conn.connect();
        int responseCode = conn.getResponseCode();
        if (!(responseCode == 200)) {
          latency = System.currentTimeMillis() - httpStartTime;
          errorMessage(conn);
        } else {
          latency = System.currentTimeMillis() - httpStartTime;
          getResponseMessage(conn);
        }
        latch.countDown();
        int gap = 0;
        if (frequencys.hasNext()) {
          gap = (int) frequencys.next();
        }
        Thread.sleep(gap);
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    } else {
      String urlString = url.toString();
      String temp = urlString.substring(urlString.indexOf("local%3A%2F%2F") + 14, urlString.indexOf("&"));
      String tmp = temp.replaceAll("%2F", "/");
      HttpURLConnection conn = null;
      URL retryUrl = null;
      httpStartTime = System.currentTimeMillis();
      try {
        conn = (HttpURLConnection) url.openConnection();
        conn = preparePost(conn, new File(tmp));
        conn.connect();
        try (FileInputStream fin = new FileInputStream(tmp); OutputStream stream = conn.getOutputStream()) {
          byte[] buffer = new byte[1024 * 1024];
          int idx = 0;
          while ((idx = fin.read(buffer)) != -1) {
            stream.write(buffer, 0, idx);
            stream.flush();
          }
        }
      } catch (Exception ex) {
      } finally {
        if (conn != null) {
          int status = conn.getResponseCode();
          if (checkRedirect(status)) {
            String retryHeaderField = conn.getHeaderField("Location");
            retryUrl = new URL(retryHeaderField);
          }
          conn.disconnect();
        }
      }
      if (retryUrl != null) {
        try {
          conn = (HttpURLConnection) retryUrl.openConnection();
          conn = preparePost(conn, new File(tmp));
          conn.connect();
          try (FileInputStream fin = new FileInputStream(tmp); OutputStream stream = conn.getOutputStream()) {
            byte[] buffer = new byte[1024 * 1024];
            int idx = 0;
            while ((idx = fin.read(buffer)) != -1) {
              stream.write(buffer, 0, idx);
              stream.flush();
            }
          }
        } finally {
          if (conn != null) {
            int responseCode = conn.getResponseCode();
            if (!(responseCode == 200)) {
              latency = System.currentTimeMillis() - httpStartTime;
              errorMessage(conn);
            } else {
              latency = System.currentTimeMillis() - httpStartTime;
              getResponseMessage(conn);
            }
            conn.disconnect();
          }
        }
      }
      latch.countDown();
      int gap = 0;
      if (frequencys.hasNext()) {
        gap = (int) frequencys.next();
      }
      Thread.sleep(gap);
    }
  }

  private void interruptThreadSleep() throws InterruptedException {
    while (Thread.interrupted()) {
      Thread.sleep(1);
    }
  }

  private void getResponseMessage(HttpURLConnection conn) throws IOException, InterruptedException {
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      StringBuilder response = new StringBuilder();
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      resParseToQueue(response.toString());
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
      resParseToQueue(errresponse.toString());
    }
  }

  public HttpURLConnection preparePost(HttpURLConnection conn, File file) throws ProtocolException {
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setDoInput(true);
    conn.setChunkedStreamingMode(1024 * 1024);
    conn.setUseCaches(false);
    conn.setConnectTimeout(60000);
    conn.setRequestProperty("User-Agent", "CodeJava Agent");
    conn.setRequestProperty("Content-Type", "application/octet-stream;");
    conn.setRequestProperty("Connection", "Keep-Alive");
    conn.setRequestProperty("Content-length", String.valueOf(file.length()));
    return conn;
  }

  private static boolean checkRedirect(int status) throws IOException {
    boolean redirect = false;
    if (status == 500) {
    } else if (status != HttpURLConnection.HTTP_OK) {
      if (status == HttpURLConnection.HTTP_MOVED_TEMP
              || status == HttpURLConnection.HTTP_MOVED_PERM
              || status == HttpURLConnection.HTTP_SEE_OTHER
              || status == 307) {
        redirect = true;
      }
    }
    return redirect;
  }

  private static Map<String, String> getQueryMap(String query) {
    String[] params = query.split("&");
    Map<String, String> map = new HashMap<>();
    for (String param : params) {
      String name = param.split("=")[0];
      String value = param.split("=")[1];
      map.put(name, value);
    }
    return map;
  }

  private void resParseToQueue(String response) {
    response = response.replaceAll("}", "");
    if (response.contains("SUCCEED")) {
      String[] results = response.split(",");
      String serverName = results[2];
      String from = results[4].substring(5).trim();
      String to = results[5].substring(3).trim();
      String elapsed = results[9].substring(10).trim();
      String transferredsize = results[11].substring(18).trim();
      StringBuilder fromBuilder = new StringBuilder();
      fromBuilder.append(COMMA).append(COLON).append("from")
              .append(COLON).append(COMMA).append(from);
      StringBuilder toBuilder = new StringBuilder();
      toBuilder.append(COMMA).append(COLON).append("to")
              .append(COLON).append(COMMA).append(to);
      StringBuilder elapsedBuilder = new StringBuilder();
      elapsedBuilder.append(COMMA).append(COLON).append("elapsed").append(COLON)
              .append(COMMA).append(elapsed).append(COLON);
      StringBuilder transferredsizeBuilder = new StringBuilder();
      transferredsizeBuilder.append(COMMA).append(COLON).append("transferredsize")
              .append(COLON).append(COMMA).append(COLON).append(transferredsize).append(COLON);
      String fromToString = fromBuilder.toString().replaceAll("\":", "");
      String toToString = toBuilder.toString().replaceAll("\":", "");
      String elapsedToInteger = elapsedBuilder.toString().replaceAll("\":", "");
      String transferSizeToInteger = transferredsizeBuilder.toString().replaceAll("\":", "");
      String csvResult = serverName.replaceAll(":", ",") + fromToString + toToString + elapsedToInteger + transferSizeToInteger;
      double throughputToDouble = Double.valueOf(transferredsize.replaceAll("\"", ""));
      ResponseItems responses = new ResponseItems(Integer.valueOf(transferredsize.replaceAll("\"", "")),
              (int) latency, "", csvResult, serverName, throughputToDouble);
      res.setResponseQueue(responses);
      res.setSchedulerQueue(responses);
    } else {
      ResponseItems responses = new ResponseItems(0,
              0, response, "", "", 0.0);
      res.setResponseQueue(responses);
      res.setSchedulerQueue(responses);
    }
  }

}
