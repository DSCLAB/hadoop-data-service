/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.exceptions.LocalDataStorageException;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriRequest;
import umc.cdc.hds.uri.UriRequestExchange;

/**
 *
 * @author brandboat
 */
public class LocalDataStorage implements DataStorage {

  public LocalDataStorage(Configuration conf) {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public DataStorageOutput create(UriRequest uri) throws IOException {
    return new UserDataOutput(uri);
  }

  @Override
  public DataStorageInput open(UriRequest uri) throws IOException {
    return new UserDataInput(uri);
  }

  @Override
  public CloseableIterator<BatchDeleteRecord> batchDelete(UriRequest uri, boolean useWild) throws IOException {
    throw new LocalDataStorageException("Not supported .");
  }

  @Override
  public DataRecord delete(UriRequest uri, boolean isRecursive) throws IOException {
    throw new LocalDataStorageException("Not supported .");
  }

  @Override
  public CloseableIterator<DataRecord> list(UriRequest uri, Query q) throws IOException {
    throw new LocalDataStorageException("Not supported .");
  }

  @Override
  public long getConnectionNum() {
    return 0;
  }

  public class UserDataInput extends DataStorageInput {

    private final UriRequestExchange parser;
    private final HttpExchange exchange;
    private final InputStream is;

    public UserDataInput(UriRequest p) throws IOException {
      this.parser = (UriRequestExchange) p;
      this.exchange = this.parser.getHttpExchange();
      if (!exchange.getRequestMethod().equals("POST")) {
        throw new IOException("User Upload should be POST.");
      }
      this.is = exchange.getRequestBody();
    }

    @Override
    public void close() throws IOException {
      is.close();
    }

    @Override
    public int read() throws IOException {
      return is.read();
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      return is.read(b, offset, length);
    }

    @Override
    public void recover() {
    }

    @Override
    public long getSize() {
      try {
        return Long.parseLong(
            exchange.getRequestHeaders().getFirst("Content-Length"));
      } catch (NumberFormatException ex) {
        return 0;
      }
    }

    @Override
    public String getName() {
      return parser.getName().orElse(null);
    }

    @Override
    public String getUri() {
      return parser.toString();
    }

  }

  public class UserDataOutput extends DataStorageOutput {

    private final UriRequestExchange parser;
    private final HttpExchange exchange;
    private final Headers headers;
    private int writeSize = 0;
    private final OutputStream os;
    // flag that presents if the response header is sent.
    // Since there may have error response to sent back.
    private boolean responseHeaderNotSent = true;

    public UserDataOutput(UriRequest p) throws IOException {
      this.parser = (UriRequestExchange) p;
      this.exchange = parser.getHttpExchange();
      this.headers = this.exchange.getResponseHeaders();
      this.os = exchange.getResponseBody();
      this.headers.set("content-disposition", "attachment; filename=\""
          + parser.getName().orElse(
              HDSConstants.DEFAULT_DOWNLOAD_FILE_NAME)
          + "\"");
    }

    @Override
    public void close() throws IOException {
      os.close();
    }

    @Override
    public void recover() throws IOException {
    }

    @Override
    public void write(int b) throws IOException {
      sendResponseHeader();
      if(parser.getName().toString().contains(".csv") && writeSize==0){ // If datatype is csv, add BOM.
        os.write(0xfeff);
      }
      os.write(b);
      writeSize++;
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      sendResponseHeader();
      byte[] BOM = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
      if(parser.getName().toString().contains(".csv") && writeSize==0){ // If datatype is csv, add BOM.
        os.write(BOM);
      }
      os.write(b, offset, length);
      writeSize += length;
    }

    @Override
    public long getSize() {
      return writeSize;
    }

    @Override
    public String getName() {
      return parser.getName()
          .orElse(HDSConstants.DEFAULT_DOWNLOAD_FILE_NAME);
    }

    @Override
    public String getUri() {
      return parser.toString();
    }

    private void sendResponseHeader() throws IOException {
      if (responseHeaderNotSent) {
        this.exchange.sendResponseHeaders(
            HDSConstants.HTTP_OK_STATUS, 0);
        responseHeaderNotSent = false;
      }
    }

    @Override
    public void commit() throws IOException {

    }

  }
}
