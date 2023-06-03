/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.IOException;
import umc.cdc.hds.datastorage.status.DataRecord;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.exceptions.HttpDataStorageException;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class HttpDataStorage implements DataStorage {

  private final Logger LOG = Logger.getLogger(this.getClass().getName());
  private static final String BOUNDRAY = "UMCCDC" + UUID.randomUUID().toString();
  private static final String PREFIX = "--";
  private static final String CRLF = "\r\n";

  public HttpDataStorage(Configuration conf) {
  }

  @Override
  public CloseableIterator<DataRecord> list(UriRequest uri, Query optArg) throws IOException {
    throw new HttpDataStorageException("Illogical!");
  }

  @Override
  public DataStorageOutput create(UriRequest uri) throws IOException {
    return new HttpDataStorageOutput(uri);
  }

  @Override
  public DataStorageInput open(UriRequest uri) throws IOException {
    return new HttpDataStorageInput(uri);
  }

  @Override
  public CloseableIterator<BatchDeleteRecord> batchDelete(UriRequest uri, boolean useWild) throws IOException {
    throw new HttpDataStorageException("Illogical!");
  }

  @Override
  public DataRecord delete(UriRequest uri, boolean isRecursive) throws IOException {
    throw new HttpDataStorageException("Illogical!");
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public long getConnectionNum() {
    return 0;
  }

  private class HttpDataStorageOutput extends DataStorageOutput {

    private long writeSize;
    private final HttpURLConnection connection;
    private final OutputStream out;
    private final String fileName;
    private final UriRequest uri;

    public HttpDataStorageOutput(UriRequest uri) throws IOException {
      this.uri = UriParser.valueOf(uri);
      fileName = this.uri.getName().orElse(HDSConstants.DEFAULT_HTTP_POST_FILE_NAME);
      uri.setName(null);
      writeSize = 0;
      connection = (HttpURLConnection) new URL(this.uri.toRealUri()).openConnection();
      connection.setRequestMethod("POST");
      connection.setConnectTimeout(0);
      connection.setRequestProperty("Content-Type", "multipart/form-data"
          + ";boundary=" + BOUNDRAY);
      connection.setRequestProperty("cache-control", "no-cache");
      connection.setDoOutput(true);
      connection.connect();
      out = connection.getOutputStream();
      out.write(setBodyHead().getBytes());
      out.flush();
    }

    @Override
    public void close() throws IOException {
      int reply = connection.getResponseCode();
      if (!(reply < 400)) {
        String error = connection.getResponseMessage();
        connection.disconnect();
        throw new HttpDataStorageException(error);
      }
      //used for debug
      try (InputStream in = connection.getInputStream()) {
        //used for debug
        byte[] buffer = new byte[1024];
        int nowRead;
        while ((nowRead = in.read(buffer)) > 0) {
          byte[] realRead = new byte[1024];
          System.arraycopy(buffer, 0, realRead, 0, nowRead);
          LOG.debug(new String(realRead));
        }
      }
      connection.disconnect();
    }

    @Override
    public void recover() throws IOException {
      out.close();
      connection.disconnect();
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      writeSize++;
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      out.write(b, offset, length);
      writeSize += length;
    }

    @Override
    public long getSize() {
      return writeSize;
    }

    @Override
    public String getName() {
      return connection.toString();
    }

    private String setBodyHead() {
      StringBuilder body = new StringBuilder();
      body.append(PREFIX).append(BOUNDRAY).append(CRLF);
      body.append("Content-Disposition: form-data; name=\"file\"; filename=\"");
      body.append(fileName).append("\"").append(CRLF);
      body.append("Content-Type: application/octet-stream").append(CRLF);
      body.append("Content-Transfer-Encoding: binary");
      body.append(CRLF).append(CRLF);
      return body.toString();
    }

    private String setBodyTail() {
      StringBuilder body = new StringBuilder();
      body.append(CRLF).append(PREFIX).append(BOUNDRAY).append(PREFIX).append(CRLF);
      return body.toString();
    }

    @Override
    public String getUri() {
      return uri.toString();
    }

    @Override
    public void commit() throws IOException {
      out.write(setBodyTail().getBytes());
      out.flush();
      out.close();
    }

  }

  private class HttpDataStorageInput extends DataStorageInput {

    private final HttpURLConnection connection;
    private InputStream in;
    private final UriRequest uri;

    public HttpDataStorageInput(UriRequest uri) throws IOException {
      this.uri = UriParser.valueOf(uri);
      connection = (HttpURLConnection) new URL(this.uri.toRealUri()).openConnection();
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(0);
      connection.connect();
      int reply = connection.getResponseCode();
      if (!(reply < 400)) {
        String error = connection.getResponseMessage();
        connection.disconnect();
        throw new HttpDataStorageException(error);
      }
      in = connection.getInputStream();
    }

    @Override
    public void close() throws IOException {
      in.close();
      int reply = connection.getResponseCode();
      if (!(reply < 400)) {
        String error = connection.getResponseMessage();
        connection.disconnect();
        throw new HttpDataStorageException(error);
      }
      connection.disconnect();
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      return in.read(b, offset, length);
    }

    @Override
    public long getSize() {
      return connection.getContentLengthLong();
    }

    @Override
    public String getName() {
      return connection.toString();
    }

    @Override
    public void recover() {

    }

    @Override
    public String getUri() {
      return uri.toString();
    }
  }
}
