package com.dslab.drs.hdsclient;

import com.dslab.drs.exception.HdsListException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.dslab.drs.exception.UploadNecessaryFilesException;
import com.dslab.drs.exception.DrsHdsAuthException;
import com.dslab.drs.exception.GetJsonBodyException;
import com.dslab.drs.restful.api.json.JsonSerialization;
import com.dslab.drs.restful.api.json.JsonUtils;
import com.dslab.drs.simulater.DrsArguments;
import com.dslab.drs.utils.DrsConfiguration;
import static com.dslab.drs.utils.DrsConfiguration.DISPATCH_HDS_LIST_ADDRESS;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author caca
 */
public class HdsClient {

  private static final Log LOG = LogFactory.getLog(HdsClient.class);

  // http header
//  private boolean needRetryByLoading = false;
//  ArrayList<String> retryLoadingFailHosts = new ArrayList();
  private String address;

//  private ArrayList<String> OutputFiles = new ArrayList<>();
//  private ArrayList<String> OutputFilesUrl = new ArrayList<>();
  private YarnConfiguration conf;

  //curl -X GET  http://140.116.245.133:8000/dataservice/v1/list?from=ftp://\$ftp-chia/ --data-urlencode enablewildcard=true
  public HdsClient(String address) {
    this.address = address;
  }

  public HdsClient(YarnConfiguration conf) {
    this.address = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS,DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT);
    this.conf = conf;
  }

  public void setConf(Configuration conf) {
    this.conf = new YarnConfiguration(conf);
  }

  public boolean getClientData(String TaskUrl, String localTaskDir) throws MalformedURLException, IOException, Exception {

    String response;
    boolean getFile = true;
    boolean redirect = false;
    boolean retry = true;

    String TaskName = FilenameUtils.getName(TaskUrl);

    String requestURL = prepareRequestUrl(address, TaskUrl, "local:///" + TaskName);
//    System.out.println(requestURL);
//    System.out.println("JRIExecutor In HDS_Get_File: prepare connection");
    URL url = new URL(requestURL);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setConnectTimeout(60000);
    con.setReadTimeout(60000);
    int status = 0;

    try {
      status = con.getResponseCode();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      String hdsErrorMessage = "";
      hdsErrorMessage = getHdsErrorMessage(con);
      throw new IOException(hdsErrorMessage);
    }

    if (status != HttpURLConnection.HTTP_OK) {
      if (isRedirect(status)) {
        redirect = true;
      }
    } else if (status == 500) {
      getFile = false;
      return getFile;
    }

    if (redirect) {
      // get redirect url from "location" header field
      String newUrl = con.getHeaderField("Location").concat(getTokenSuffix());
      con = (HttpURLConnection) new URL(newUrl).openConnection();
    }
    File targetfile = new File(localTaskDir + TaskName);

    long NanoSec = 0;
    NanoSec = System.nanoTime();
    if (!targetfile.exists()) {
      try (InputStream stream = con.getInputStream()) {
        Files.copy(stream, Paths.get(localTaskDir + TaskName));
      } catch (Exception e) {
        LOG.error("HdsClient get file encounter error");
        e.printStackTrace();
        getFile = false;
        String hdsErrorMessage = getHdsErrorMessage(con);
        if (isTaskFull(hdsErrorMessage)) {
          throw new Exception("Download file encounter error :" + hdsErrorMessage);
        }
      }
      response = getResponse(con);
      if (response.toString().startsWith("500")) {
        getFile = false;
        String hdsErrorMessage = "";
        if (hdsErrorMessage == "") {
          hdsErrorMessage = getHdsErrorMessage(con);
        }
      }
    }
    return getFile;
  }

  public String utf8Encode(String input) {
    try {
      return URLEncoder.encode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  public boolean isTaskFull(String Response) {
    if (Response.contains("Task is full now")) {
      return true;
    }
    return false;
  }

  private boolean isRedirect(int status) {
    return (status == HttpURLConnection.HTTP_MOVED_TEMP
            || status == HttpURLConnection.HTTP_MOVED_PERM
            || status == HttpURLConnection.HTTP_SEE_OTHER
            || status == 307);
  }

  public URL createAccessUrl(String from, String to, DrsConfiguration conf) throws MalformedURLException {
    String address = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT);
    String tokenSuffix = getTokenSuffix(conf);
    String accessUrl = address.concat("?from=").concat(utf8Encode(from)).concat("&to=").concat(utf8Encode(to)).concat(tokenSuffix);
    return new URL(accessUrl);
  }
  
  
  public URL createUpdloadUrl(String local, String to, DrsConfiguration conf) throws MalformedURLException {
    String address = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT);
    String tokenSuffix = getTokenSuffix(conf);
    String fileName = new File(local).getName();
    String from = "local:///" + fileName;
    String post = address.concat("?from=").concat(utf8Encode(from)).concat("&to=").concat(utf8Encode(to)).concat(tokenSuffix);
    return new URL(post);
  }

  public URL createDeleteUrl(String target, DrsConfiguration conf) throws MalformedURLException {
    String deleteAddress = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS,
            DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT)
            .replaceAll("access", "delete");

    return new URL(deleteAddress.concat("?from=").concat(target).concat(getTokenSuffix()));
  }

  public URL createRetryUrl(String header, DrsConfiguration conf) throws MalformedURLException {
    String tokenSuffix = getTokenSuffix(conf);
    String retryUrl = header.concat(tokenSuffix);
    return new URL(retryUrl);
  }

  public URL createDrsRunUrl(DrsArguments drsArguments, Configuration conf, boolean async) throws MalformedURLException {
    String runAddress = conf.get(DrsConfiguration.DISPATCH_HDS_RUN_ADDRESS, DrsConfiguration.DISPATCH_HDS_RUN_ADDRESS_DEFAULT);
    String tokenSuffix = getTokenSuffix(conf);
    String codeArgument = "&code=" + utf8Encode(drsArguments.getCode());
    String dataArgument = "&data=" + utf8Encode(drsArguments.getData());
    String configArgument = "&config=" + utf8Encode(drsArguments.getConfig());
    String codeoutArgument = "&codeout=" + utf8Encode(drsArguments.getCodeout());
    String copytoArgument = "&copyto=" + utf8Encode(drsArguments.getCopyto());

    StringBuilder runUrl = new StringBuilder(runAddress).append(tokenSuffix).append(codeArgument).append(dataArgument).append(configArgument).append(codeoutArgument)
            .append(copytoArgument);
    if (drsArguments.getConsoleto() != null) {
      String consoleToArgument = "&consoleto=" + utf8Encode(drsArguments.getConsoleto());
      runUrl.append(consoleToArgument);
    }
    if (async) {
      runUrl.append("&async=true");
    }

    return new URL(runUrl.toString());
  }

  public URL createDrsKillUrl(String applicationId, Configuration conf) throws MalformedURLException {
    String drsKillAddress = conf.get(DrsConfiguration.DISPATCH_HDS_KILL_ADDRESS, DrsConfiguration.DISPATCH_HDS_KILL_ADDRESS_DEFAULT);
    String appIdArgument = "applicationID=" + applicationId;
    String killUrl = drsKillAddress + appIdArgument;
    return new URL(killUrl);
  }

  public URL createDrsWatchUrl(String applicationId, Configuration conf) throws MalformedURLException {
    String drsWatchAddress = conf.get(DrsConfiguration.DISPATCH_HDS_WATCH_DRS_ADDRESS, DrsConfiguration.DISPATCH_HDS_WATCH_DRS_ADDRESS_DEFAULT);
    String appIdArgument = "applicationID=" + applicationId;
    String watchDrsUrl = drsWatchAddress + appIdArgument;
    return new URL(watchDrsUrl);
  }

  public URL createListUrl(String path, DrsConfiguration conf) throws MalformedURLException, IOException {
    String listAddress = conf.get(DISPATCH_HDS_LIST_ADDRESS);
    String tokenSuffix = getTokenSuffix(conf);
    String fileName = FilenameUtils.getName(path);
    String filePath = FilenameUtils.getPath(path);
    String listUrl = "";

    if (filePath.length() >= 3) {
      if (filePath.substring(0, 3).equals("hds")) {
        listUrl = filePath + "?key=" + utf8Encode(fileName);
      } else {
        listUrl = filePath + "?name=" + utf8Encode(fileName);
      }
    } else {
      throw new IOException("List path error:" + path);
    }

    String encodedURL = utf8Encode(listUrl);
    return new URL(listAddress + encodedURL.concat(tokenSuffix));
  }

  public void hdsAccessService(String fromUrl, String toUrl) throws UploadNecessaryFilesException {
//    LOG.info("Get \""+DRSConstants.DRS_HDS_AUTH_TOKEN+"\":" + conf.get(DRSConstants.DRS_HDS_AUTH_TOKEN, ""));
    if (!toUrl.startsWith("hdfs")) {
      toUrl = "hdfs://" + toUrl;
    }

    LOG.debug("HdsClient.hdsAccessService:" + address.concat("?from=").concat(fromUrl).concat("&to=").concat(toUrl).concat(getTokenSuffix()));
    HttpURLConnection connection = null;
    InputStream is = null;
    try {
      URL url = new URL(address.concat("?from=").concat(fromUrl).concat("&to=").concat(toUrl).concat(getTokenSuffix()));
      LOG.info("hdsAccessService :" + address.concat("?from=").concat(fromUrl).concat("&to=").concat(toUrl).concat(getTokenSuffix()));
      connection = (HttpURLConnection) url.openConnection();
      is = connection.getInputStream();

    } catch (IOException ex) {
      LOG.info("hdsAccessService encounter error IOException :" + ex.getMessage());
      String hdsAccessServiceErrorMessage = getHdsErrorMessage(connection);
      LOG.info("hdsAccessServiceErrorMessage :" + hdsAccessServiceErrorMessage);
      LOG.info("hdsAccessServiceErrorMessage is null :" + (hdsAccessServiceErrorMessage == null));
      if (hdsAccessServiceErrorMessage == null) {
        hdsAccessServiceErrorMessage = ex.getMessage();
      }
      throw new UploadNecessaryFilesException(hdsAccessServiceErrorMessage);
    } catch (Exception e) {
      LOG.info("hdsAccessService encounter error Exception :" + e.getMessage());
      String hdsAccessServiceErrorMessage = getHdsErrorMessage(connection);
      LOG.info("hdsAccessServiceErrorMessage :" + hdsAccessServiceErrorMessage);
      throw new UploadNecessaryFilesException(hdsAccessServiceErrorMessage);
    } finally {
      try {
        is.close();
        connection.disconnect();
      } catch (IOException | NullPointerException ex) {
        LOG.info("hdsAccessService encounter error while closing resource");
        LOG.info("Exception :" + ex.getMessage());
      }

    }

  }

  private String getTokenSuffix() {
    String auth_token = conf.get(
            DrsConfiguration.DRS_HDS_AUTH_TOKEN,
            DrsConfiguration.DRS_HDS_AUTH_TOKEN_DEFAULT);
    return auth_token.equals("") ? "" : "&token=" + auth_token;
  }

  private String getTokenSuffix(Configuration conf) {
    String auth_token = conf.get(
            DrsConfiguration.DRS_HDS_AUTH_TOKEN,
            DrsConfiguration.DRS_HDS_AUTH_TOKEN_DEFAULT);
    return auth_token.equals("") ? "" : "&token=" + auth_token;
  }

//  public ArrayList<String> getOutputFiles() {
//    return this.OutputFiles;
//  }
  public String checkCopytoEnd(String Copyto) {
    if (!Copyto.endsWith("/")) {
      Copyto = Copyto.concat("/");
    }
    return Copyto;
  }

  public boolean checkSuceessStatusCode(HttpURLConnection conn) throws IOException {
    boolean result = false;

    int status = checkStatusCode(conn);
    if (status == 500) {
      LOG.debug("status == 500,getHdsErrorMessage:" + getHdsErrorMessage(conn));
    } else if (status != HttpURLConnection.HTTP_OK) {
      if (isRedirect(status)) {
      }
    } else if (status == 200) {
      result = true;
    }

    return result;
  }

  public boolean checkRedirect(HttpURLConnection conn) throws IOException {
    boolean redirect = false;

    int status = checkStatusCode(conn);
    if (status == 500) {
//            System.out.println("status == 500");
//            System.out.println(getHdsErrorMessage(conn));
    } else if (status != HttpURLConnection.HTTP_OK) {
      if (isRedirect(status)) {
        redirect = true;
      }
    }

//    System.out.println("status in checkRedirect :" + status);
    return redirect;
  }

  public boolean checkRedirect(int status) throws IOException {
    if (isRedirect(status)) {
      return true;
    }
    return false;
  }

  public int checkStatusCode(HttpURLConnection conn) throws IOException {
    boolean redirect;

    int status = conn.getResponseCode();
    conn.connect();
//    System.out.println("status:" + status);

    return status;
  }

  public String prepareRequestUrl(String address, String from, String to) {
    String request = address.concat("?from=").concat(utf8Encode(from)).concat("&to=").concat(utf8Encode(to)).concat(getTokenSuffix());
//    System.out.println("request:" + request);
    return request;
  }

  public HttpURLConnection preparePost(HttpURLConnection conn, File file) throws ProtocolException {
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setDoInput(true);
    conn.setChunkedStreamingMode(1024 * 1024);
    conn.setUseCaches(false);
    conn.setConnectTimeout(5000);
    conn.setRequestProperty("User-Agent", "CodeJava Agent");
    conn.setRequestProperty("Content-Type", "application/octet-stream;");
    conn.setRequestProperty("Connection", "Keep-Alive");
    conn.setRequestProperty("Content-length", String.valueOf(file.length()));
    return conn;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getHdsErrorMessage(HttpURLConnection conn) {
//    System.out.println("In getHdsErrorMessage");
    BufferedReader rd = null;
    StringBuilder sb = new StringBuilder("");
    String line = null;
    boolean findresponse = true;
    try {
      InputStream stream = conn.getErrorStream();
      if (stream == null) {
        LOG.debug("conn.getErrorStream is null");
        stream = conn.getInputStream();
        if (stream == null) {
          findresponse = false;
          LOG.debug("No response find in getErrorStream and getInputStream");
        }
      }
      if (findresponse) {
        rd = new BufferedReader(new InputStreamReader(stream));
        while ((line = rd.readLine()) != null) {
          sb.append(line + '\n');
        }
      }
    } catch (Exception e) {
      LOG.error(e.toString());
      LOG.error(e.getMessage());
    }
    return sb.toString();
  }

  public String getResponse(HttpURLConnection con) throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append(con.getResponseCode())
            .append(" ")
            .append(con.getResponseMessage())
            .append("\n");

    Map<String, List<String>> map = con.getHeaderFields();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      builder.append(entry.getKey())
              .append(": ");

      List<String> headerValues = entry.getValue();
      Iterator<String> it = headerValues.iterator();
      if (it.hasNext()) {
        builder.append(it.next());

        while (it.hasNext()) {
          builder.append(", ")
                  .append(it.next());
        }
      }

      builder.append("\n");
    }
    return builder.toString();
  }

  public boolean checkHdsFileExist(YarnConfiguration conf, String path) throws HdsListException, DrsHdsAuthException {
//    LOG.info("inner checkHdsFileExist()");
//    LOG.info("path:" + path);
    this.conf = conf;

    String listAddress = conf.get("dispatch.HDS.list.address");
    LOG.info("listAddress:" + listAddress);

    String fileName = FilenameUtils.getName(path);
    String filePath = FilenameUtils.getPath(path);
    String listUrl = "";
    LOG.info(filePath);

    if (filePath.length() >= 3) {
      if (filePath.substring(0, 3).equals("hds")) {
        listUrl = filePath + "?key=" + utf8Encode(fileName);
      } else {
        listUrl = filePath + "?name=" + utf8Encode(fileName);
      }
    } else {
      throw new HdsListException("File not found at path:" + path);
    }

    String encodedURL = utf8Encode(listUrl);
//    String encodedURL = listUrl;
    LOG.info(listAddress + encodedURL);
    long size = 0;
    try {
      String jsonStr = getJSON(listAddress + encodedURL, getTokenSuffix());
      size = getSizeFromJSON(jsonStr);
    } catch (DrsHdsAuthException ex) {
      LOG.info("checkHdsFileExist getJSON error");
      LOG.info(ex.getMessage());
      throw new DrsHdsAuthException(ex.getMessage());
    } catch (Exception ex) {
      LOG.info("checkHdsFileExist getJSON error");
      LOG.info(ex.getMessage());

      LOG.info(ex.toString());
    }
    if (size > 0) {
      return true;
    } else {
      throw new HdsListException("File not found at path:" + path);
    }
  }

//  public boolean isNeedRetryByLoading() {
//    return this.needRetryByLoading;
//  }
  public boolean checkHdsDirectoryExist(YarnConfiguration conf, String path) throws HdsListException, DrsHdsAuthException {
//    LOG.info("inner checkHdsFileExist()");
//    LOG.info("path:" + path);
    this.conf = conf;

    String listAddress = conf.get("dispatch.HDS.list.address");
    LOG.info("listAddress:" + listAddress);

    String fileName = FilenameUtils.getName(path);
    String filePath = FilenameUtils.getPath(path);
    String listUrl = "";
    LOG.info(filePath);

    if (filePath.length() >= 3) {
      listUrl = filePath;
    } else {
      throw new HdsListException("Directory not found at path:" + path);
    }

    String encodedURL = utf8Encode(listUrl);
    LOG.info(listAddress + encodedURL);
    long size = 0;
    try {
      //GSON 是否需要二次編碼?
      String jsonStr = getJSON(listAddress + encodedURL, getTokenSuffix());
      size = getSizeFromJSON(jsonStr);
    } catch (DrsHdsAuthException ex) {
      LOG.info("checkHdsFileExist getJSON error");
      LOG.info(ex.getMessage());
      throw new DrsHdsAuthException(ex.getMessage());
    } catch (Exception ex) {
      LOG.info("checkHdsDirectoryExist getJSON error");
      LOG.info(ex.getMessage());
    }
    if (size > 0) {
      return true;
    } else {
      throw new HdsListException("Directory is empty :" + path);
    }
  }

  public URL getNewUrl(HttpURLConnection conn) throws MalformedURLException {
    String newUrl = conn.getHeaderField("Location").concat(getTokenSuffix());
    if (newUrl == null) {
      LOG.error("In getNewUrl newUrl=null! return null");
      return null;
    } else {
      return new URL(newUrl);
    }
  }

  public URL getRedirectionUrl(String connectionHeaderFilder) throws MalformedURLException {
    String newUrl = connectionHeaderFilder.concat(getTokenSuffix());
    return new URL(newUrl);
  }

//  public void recordUrl(String destination) {
//    OutputFilesUrl.add(destination);
//  }
  public long getSizeFromJSON(String jsonStr) {
    long size;
    try {
      JSONResult result = JsonUtils.fromJson(jsonStr, JSONResult.class);
      size = Long.parseLong(result.getDataInfo().get(0).getSize());

    } catch (Exception e) {
      return 0;
    }

    return size;
  }

  public String getJSON(String url, String token) throws DrsHdsAuthException, GetJsonBodyException {
    StringBuilder sb = new StringBuilder();
    url = url.concat(token);
    HttpURLConnection conn = null;
    try {
      LOG.info("Reaal Url:" + url);
      URL urlUse = new URL(url);
      conn = (HttpURLConnection) urlUse
              .openConnection();
      BufferedReader in = new BufferedReader(new InputStreamReader(
              conn.getInputStream()));
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        sb.append(inputLine);
      }
      in.close();
    } catch (Exception e) {
      String errorMessage = "";
      errorMessage = getHdsErrorMessage(conn);
      LOG.info("GetJSON exception :" + getHdsErrorMessage(conn));
      LOG.info(e.toString());
      if (errorMessage.contains("AuthException")) {
        throw new DrsHdsAuthException(errorMessage);
      } else {
        throw new GetJsonBodyException(e.toString());
      }
    }
    return sb.toString();
  }

  public static class JSONResult implements JsonSerialization {

    private List<dataInfo> dataInfo;

    public List<dataInfo> getDataInfo() {
      return this.dataInfo;
    }
  }

  public class dataInfo {

    private String uri;
    private String location;
    private String name;
    private String size;
    private String ts;
    private String type;
    private List<dataowner> dataowner;

    public String getUri() {
      return this.uri;
    }

    public String getLocation() {
      return this.location;
    }

    public String getName() {
      return this.name;
    }

    public String getSize() {
      return this.size;
    }

    public String getTs() {
      return this.ts;
    }

    public String getType() {
      return this.type;
    }

    public List<dataowner> getDataowner() {
      return this.dataowner;
    }

  }

  public class dataowner {

    private String host;
    private String ratio;

    public String getHost() {
      return this.host;
    }

    public String getRatio() {
      return this.ratio;
    }
  }

}
