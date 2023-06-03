/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.Response.Status;
import org.testng.annotations.Test;
import umc.cdc.hds.datastorage.status.ErrorInfo;
import umc.cdc.hds.httpserver.ResponseBuilder.Format;
import umc.cdc.hds.httpserver.ResponseBuilder.Response;

/**
 *
 * @author brandboat
 */
public class ResponseNGTest {

  @Test
  public void test() throws Exception {
    Response response = new ResponseBuilder()
        .setBufferLimit(1024)
        .setContent(new Result(Status.INTERNAL_SERVER_ERROR, new ErrorInfo(new IOException("haha!")), "brandboat"))
        .setFormat(Format.json)
        .setExchange(new Exchange(null)).build();
    response = new ResponseBuilder().setBufferLimit(1024)
        .setContent(new Result(Status.INTERNAL_SERVER_ERROR, new ErrorInfo(new IOException("yaya!")), "YOYOYYO"))
        .setExchange(new Exchange(null))
        .setFormat(Format.json).build();

    InputStream is = response.getInput();
    int i = 0;
    byte[] buffer = new byte[1024];
    while ((i = is.read(buffer)) >= 0) {
    }
    System.out.println(new String(buffer));
  }

}
