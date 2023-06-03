/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import com.sun.net.httpserver.Headers;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import umc.cdc.hds.tools.InfiniteOutputStream;

/**
 *
 * @author brandboat
 */
public class ResponseBuilder {

  private Result result;
  private int bufferLimit;
  private Format format;
  private Exchange exchange;
  private byte[] buffer;

  public enum Format {

    json;
  }

  public ResponseBuilder setContent(Result r) {
    result = r;
    return this;
  }

  public ResponseBuilder setBufferLimit(int limit) {
    bufferLimit = limit;
    return this;
  }

  public ResponseBuilder setBuffer(byte[] byteArray) {
    buffer = byteArray;
    bufferLimit = byteArray.length;
    return this;
  }

  public ResponseBuilder setFormat(Format f) {
    format = f;
    return this;
  }

  public ResponseBuilder setExchange(Exchange ex) {
    exchange = ex;
    return this;
  }

  public Response build() throws IOException {

    if (result == null) {
      throw new IOException("result == null");
    }

    if (bufferLimit <= 0) {
      throw new IOException("bufferLimit < 0");
    }

    if (format == null) {
      throw new IOException("format not specified.");
    }

    if (exchange == null) {
      throw new IOException("exchange not specified.");
    }

    if (buffer == null) {
      return new Response(exchange, format, bufferLimit, result);
    } else {
      return new Response(exchange, format, buffer, result);
    }
  }

  public static class Response implements Closeable {

    private final Status httpStatusCode;
    private final Format format;
    private final InfiniteOutputStream.InfiniteDumpLocation idl;
    private final InfiniteOutputStream infinite;
    private final Formatter formatter;
    private final int bufferLimit;
    private final Exchange exchange;
    private final Result result;
    private static final Log LOG = LogFactory.getLog(Response.class);

    public Response(Exchange exchange, Format format, int bufferLimit, Result result) throws IOException {
      this.format = format;
      this.bufferLimit = bufferLimit;
      this.result = result;
      this.idl = new InfiniteOutputStream.LocalFile();
      this.httpStatusCode = result.getCode();
      this.infinite = new InfiniteOutputStream(bufferLimit, idl);
      this.formatter = createFormatter(format, infinite);
      this.exchange = exchange;
      getContent();
    }

    public Response(Exchange exchange, Format format, byte[] buffer, Result result) throws IOException {
      this.format = format;
      this.bufferLimit = buffer.length;
      this.result = result;
      this.idl = new InfiniteOutputStream.LocalFile();
      this.httpStatusCode = result.getCode();
      this.infinite = new InfiniteOutputStream(buffer, idl);
      this.formatter = createFormatter(format, infinite);
      this.exchange = exchange;
      getContent();
    }

    private Formatter createFormatter(Format format, OutputStream os)
        throws IOException {
      switch (format) {
        case json:
          return new JsonFormatter(os);
        default:
          throw new IOException("Unsupported response format.");
      }
    }

    private void getContent() throws IOException {
      formatter.convert(result);
      //infinite.close();
      /**
       * When JsonWriter closing , it close outputstream inside JsonWriter this
       * outputstream is infiniteOutputstream . so infiniteOutputstream
       * shouldn't close
       *
       * *Note: not every formatter will close outputstream when closing and
       * not every formatter won't close outputstream when closing
       */
      formatter.close();
    }

    public InputStream getInput() throws IOException {
      return infinite.getInputStream();
    }

    public Result getResult() {
      return result;
    }

    public int getCode() {
      return httpStatusCode.getStatusCode();
    }

    public Format getFormat() {
      return format;
    }

    @Override
    public void close() throws IOException {
      deleteTempFile();
    }

    public void send() {
      try {
        if (exchange.isSend()) {
          return;
        }
        transferResponse();
      } catch (Exception ex) {
        LOG.error(ex);
      } finally {
        exchange.close();
      }
    }

    private void deleteTempFile() throws IOException {
      if (!infinite.isInMemory()) {
        idl.delete();
      }
    }

    private void transferResponse() {

      OutputStream body = null;
      InputStream content = null;
      try {
        Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/" + getFormat());
        exchange.sendResponseHeaders(getCode(), 0);
        body = exchange.getResponseBody();
        content = getInput();
        byte[] buf = new byte[1024];
        int i = 0;
        while ((i = content.read(buf)) >= 0) {
          body.write(buf, 0, i);
        }
        body.flush();
      } catch (IOException ex) {
        LOG.error(ex);
      } catch (Exception ex) {
        LOG.error(ex);
      } finally {
        close(body);
        close(content);
      }

    }

    private static void close(Closeable closeable) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (IOException ex) {
        LOG.error(ex);
      }
    }

  }

}
