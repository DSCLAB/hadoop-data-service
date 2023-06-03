/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.httprequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author Weli
 */
public class HttpUpload implements Runnable {

  BlockingQueue<URL> reqBlockingQueue;
  int reqNumPerUser;
  private final CountDownLatch locallatch;
  BlockingQueue<String> localQueue;

  public HttpUpload(BlockingQueue<String> localQueue, BlockingQueue<URL> reqBlockingQueue,
          int reqNumPerUser, CountDownLatch locallatch) throws InterruptedException {
    this.reqBlockingQueue = reqBlockingQueue;
    this.reqNumPerUser = reqNumPerUser;
    this.locallatch = locallatch;
    this.localQueue = localQueue;
  }

  @Override
  public void run() {
    try {
      innerRun();
    } catch (IOException ex) {
      System.out.println(ex.toString());
    } catch (InterruptedException ex) {
      System.out.println(ex.toString());
    } finally {
      locallatch.countDown();
    }
  }

  private void innerRun() throws IOException, InterruptedException {
    for (int i = 0; i < reqNumPerUser; i++) {
      while (Thread.interrupted()) {
        Thread.sleep(1);
      }
      String local = localQueue.take();
      URL url = reqBlockingQueue.take();
      URL retryUrl = null;
      if (!url.toString().contains("to=local")) {
        HttpURLConnection conn = null;
        try {
          conn = (HttpURLConnection) url.openConnection();
          conn = preparePost(conn, new File(local));
          conn.connect();
          try (FileInputStream fin = new FileInputStream(local); OutputStream stream = conn.getOutputStream()) {
            byte[] buffer = new byte[1024 * 1024];
            int idx = 0;
            while ((idx = fin.read(buffer)) != -1) {
              stream.write(buffer, 0, idx);
              stream.flush();
            }
          } catch (Exception e) {
            System.out.println(e);
          }
        } catch (Exception e) {
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
            conn = preparePost(conn, new File(local));
            conn.connect();
            try (FileInputStream fin = new FileInputStream(local); OutputStream stream = conn.getOutputStream()) {
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
                errorMessage(conn);
              } else {
                getResponseMessage(conn);
              }
              conn.disconnect();
            }
          }
        }
      }
    }
  }

  public HttpURLConnection preparePost(HttpURLConnection conn, File file) throws ProtocolException {
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setInstanceFollowRedirects(false);
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

  private void getResponseMessage(HttpURLConnection conn) throws IOException, InterruptedException {
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
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
    }

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

}
