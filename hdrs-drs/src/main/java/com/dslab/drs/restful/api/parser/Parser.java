package com.dslab.drs.restful.api.parser;

import com.dslab.drs.exception.GetJsonBodyException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public final class Parser {

  public static String getJsonBodyFromUrl(String url) throws GetJsonBodyException {
    StringBuilder jsonBody = new StringBuilder();
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) new URL(url).openConnection();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          jsonBody.append(inputLine);
        }
      }
      return jsonBody.toString();
    } catch (Exception e) {
      throw new GetJsonBodyException("Can't get json body from url:" + url + "," + e.toString());
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * private ctor for util class.
   */
  private Parser() {
  }
}
