/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

/**
 *
 * @author brandboat
 */
public class Exchange extends HttpExchange {

  private final HttpExchange exchange;
  private boolean isSend = false;

  public Exchange(HttpExchange exchange) {
    this.exchange = exchange;
  }

  @Override
  public Headers getRequestHeaders() {
    return exchange.getRequestHeaders();
  }

  @Override
  public Headers getResponseHeaders() {
    return exchange.getResponseHeaders();
  }

  @Override
  public URI getRequestURI() {
    return exchange.getRequestURI();
  }

  @Override
  public String getRequestMethod() {
    return exchange.getRequestMethod();
  }

  @Override
  public HttpContext getHttpContext() {
    return exchange.getHttpContext();
  }

  @Override
  public void close() {
    exchange.close();
  }

  @Override
  public InputStream getRequestBody() {
    return exchange.getRequestBody();
  }

  @Override
  public OutputStream getResponseBody() {
    return exchange.getResponseBody();
  }

  @Override
  public void sendResponseHeaders(int i, long l) throws IOException {
    isSend = true;
    exchange.sendResponseHeaders(i, l);
  }

  @Override
  public InetSocketAddress getRemoteAddress() {
    return exchange.getRemoteAddress();
  }

  @Override
  public int getResponseCode() {
    return exchange.getResponseCode();
  }

  @Override
  public InetSocketAddress getLocalAddress() {
    return exchange.getLocalAddress();
  }

  @Override
  public String getProtocol() {
    return exchange.getProtocol();
  }

  @Override
  public Object getAttribute(String string) {
    return exchange.getAttribute(string);
  }

  @Override
  public void setAttribute(String string, Object o) {
    exchange.setAttribute(string, o);
  }

  @Override
  public void setStreams(InputStream in, OutputStream out) {
    exchange.setStreams(in, out);
  }

  @Override
  public HttpPrincipal getPrincipal() {
    return exchange.getPrincipal();
  }

  public boolean isSend() {
    return isSend;
  }

}
