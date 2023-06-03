/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.createRequest;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 *
 * @author Weli
 */
public class ListRequestCreator {

  private final String list;
  private final String from;

  public ListRequestCreator(String list, String from) {
    this.list = list;
    this.from = from;
  }

  public String createListReq() {
    StringBuilder req = new StringBuilder();
    req.append(list)
            .append("?from=")
            .append(encode(from))
            .append("&limit=-1");
    return req.toString();
  }

  private String encode(String url) {
    try {
      return URLEncoder.encode(url, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

}
