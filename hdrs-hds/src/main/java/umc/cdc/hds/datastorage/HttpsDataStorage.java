/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.exceptions.HttpDataStorageException;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;

/**
 *
 * @author jpopaholic
 */
public class HttpsDataStorage implements DataStorage {

  private final Logger LOG = Logger.getLogger(this.getClass().getName());
  private static final String BOUNDRAY = "UMCCDC" + UUID.randomUUID();
  private static final String PREFIX = "--";
  private static final String CRLF = "\r\n";

  public HttpsDataStorage(Configuration conf) {
  }

  @Override
  public CloseableIterator<DataRecord> list(UriRequest uri, Query optArg) throws IOException {
    throw new HttpDataStorageException("Illogical!");
  }

  @Override
  public DataStorageOutput create(UriRequest uri) throws IOException {
    return new HttpsDataStorageOutput(uri);
  }

  @Override
  public DataStorageInput open(UriRequest uri) throws IOException {
    return new HttpsDataStorageInput(uri);
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

  private class HttpsDataStorageOutput extends DataStorageOutput {

    private long writeSize;
    private final HttpsURLConnection connection;
    private final OutputStream out;
    private final String fileName;
    private final UriRequest uri;

    public HttpsDataStorageOutput(UriRequest uri) throws IOException {
      //建立SSLContext
      SSLContext sslContext= null;
      try {
        sslContext = SSLContext.getInstance("SSL");
      } catch (NoSuchAlgorithmException e) {
        throw new HttpDataStorageException(e.toString());
      }
      TrustManager[] tm={new MyX509TrustManager()};
      //初始化
      try {
        sslContext.init(null, tm, new java.security.SecureRandom());
      } catch (KeyManagementException e) {
        throw new HttpDataStorageException(e.toString());
      }
      //獲取SSLSocketFactory物件
      SSLSocketFactory ssf=sslContext.getSocketFactory();
      this.uri = UriParser.valueOf(uri);
      fileName = this.uri.getName().orElse(HDSConstants.DEFAULT_HTTP_POST_FILE_NAME);
      uri.setName(null);
      writeSize = 0;
      connection = (HttpsURLConnection) new URL(this.uri.toRealUri()).openConnection();
      connection.setRequestMethod("POST");
      connection.setConnectTimeout(0);
      connection.setRequestProperty("Content-Type", "multipart/form-data"
          + ";boundary=" + BOUNDRAY);
      connection.setRequestProperty("cache-control", "no-cache");
      connection.setDoOutput(true);
      connection.setSSLSocketFactory(ssf);
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

  private class HttpsDataStorageInput extends DataStorageInput {

    private final HttpsURLConnection connection;
    private final InputStream in;
    private final UriRequest uri;

    public HttpsDataStorageInput(UriRequest uri) throws IOException {
      //建立SSLContext
      SSLContext sslContext= null;
      try {
        sslContext = SSLContext.getInstance("SSL");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      TrustManager[] trustAllCerts = new TrustManager[]{
              new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                  return null;
                }
                public void checkClientTrusted(
                        java.security.cert.X509Certificate[] certs, String authType) {
                }
                public void checkServerTrusted(
                        java.security.cert.X509Certificate[] certs, String authType) {
                }
              }
      };
      TrustManager[] tm={new MyX509TrustManager()};
      //初始化
      try {
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      } catch (KeyManagementException e) {
        throw new HttpDataStorageException(e.toString());
      }
      //獲取SSLSocketFactory物件
      SSLSocketFactory ssf=sslContext.getSocketFactory();
      HostnameVerifier ignoreHostnameVerifier = new HostnameVerifier() {
        public boolean verify(String s, SSLSession sslsession) {
          //System.out.println("WARNING: Hostname is not matched for cert.");
          return true;
        }
      };
      this.uri = UriParser.valueOf(uri);
      connection = (HttpsURLConnection) new URL(this.uri.toRealUri()).openConnection();
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(0);
      HttpsURLConnection.setDefaultHostnameVerifier(ignoreHostnameVerifier);
      connection.setSSLSocketFactory(ssf);
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

  public class MyX509TrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    // TODO Auto-generated method stub
    }
    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      // TODO Auto-generated method stub
    }
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
