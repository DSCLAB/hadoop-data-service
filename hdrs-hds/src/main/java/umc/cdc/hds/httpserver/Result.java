/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import javax.ws.rs.core.Response.Status;

/**
 *
 * @author brandboat
 */
public class Result {

  private final Object result;
  private final String title;
  private final Status code;

  public Result(Status code, Object result, String title) {
    this.code = code;
    this.result = result;
    this.title = title;
  }

  public Status getCode() {
    return code;
  }

  public String getTitle() {
    return title;
  }

  public Object getContent() {
    return result;
  }
}
