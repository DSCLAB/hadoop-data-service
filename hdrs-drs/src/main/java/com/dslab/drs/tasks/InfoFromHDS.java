package com.dslab.drs.tasks;

/**
 *
 * @author kh87313
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.FilenameUtils;
import com.dslab.drs.restful.api.json.JsonSerialization;
import com.dslab.drs.restful.api.json.JsonUtils;
import com.dslab.drs.restful.api.response.list.DataInfos;
import com.dslab.drs.socket.SocketGlobalVariables;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InfoFromHDS {

  private static final Log LOG = LogFactory.getLog(InfoFromHDS.class);
  private final int schedulerBatchSize;
  List<DataInfos> listResult = new ArrayList<>();
  String urlTitle; // http://140.116.245.133:8000/dataservice/v1/list/
  private String token;
  SocketGlobalVariables socketGlobal = SocketGlobalVariables.getAMGlobalVariable();

  //Constructor
  public InfoFromHDS(String urlTitle, int schedulerBatchSize) {
    this.urlTitle = urlTitle;
    this.schedulerBatchSize = schedulerBatchSize;
  }

  public void setToken(String token) {
    if (token.equals("")) {
      this.token = "";
    } else {
      this.token = "&token=" + token;
    }
  }

  ArrayList<FileInfo> addFromDirectory(String filePathInUrl) {
    return getDirectoryListFromHds(filePathInUrl, filePathInUrl, "");
  }

  private Map<String, ArrayList<String>> readFileUrl(String filePath) throws IOException {
    Map<String, ArrayList<String>> listMap = new HashMap<>();
    try (FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr)) {
      ArrayList<String> files = new ArrayList<>();
      ArrayList<String> tablePaths = new ArrayList<>();
      ArrayList<String> urls = new ArrayList<>();
      String url;
      while ((url = br.readLine()) != null) {
        if (!url.isEmpty()) {
          url = url.trim();  //濾掉頭尾空白
//          LOG.debug(files.size() + "," + url);
          String tablePath = FilenameUtils.getPath(url);
          String file = FilenameUtils.getName(url);
//          LOG.debug(tablePath + "," + file);

          if (!urls.contains(url)) {
            urls.add(url);
            tablePaths.add(tablePath);
            files.add(file);
          } else {
            LOG.debug("file name duplicate! skip this.");
          }
        }
      }
      listMap.put("files", files);
      listMap.put("tablePaths", tablePaths);
      listMap.put("urls", urls);
    }
    return listMap;
  }

  ArrayList<FileInfo> addFromFile(String filePath) throws IOException {
    ArrayList<FileInfo> infos = new ArrayList<>();
    LOG.debug("filePath:" + filePath);
    Map<String, ArrayList<String>> listMap = readFileUrl(filePath);
    try {
      ArrayList<String> files = listMap.get("files");
      ArrayList<String> tablePaths = listMap.get("tablePaths");
      ArrayList<String> urls = listMap.get("urls");
      socketGlobal.setLastBatchAcc(socketGlobal.getBatchAcc());
      final int urlSize = urls.size();
//      lastBatchAcc = batchAcc;
      if (socketGlobal.getBatchAcc() + schedulerBatchSize < urlSize) {
        socketGlobal.increaseBatchAcc(schedulerBatchSize);
//        batchAcc += schedulerBatchSize;
      } else {
        socketGlobal.setBatchAcc(urlSize);
//        s.markListThreadFinished();
//        batchAcc = urls.size();
      }

      int lastBatchAcc = socketGlobal.getLastBatchAcc();
      int batchAcc = socketGlobal.getBatchAcc();

//      LOG.info("####lastBatchAcc=" + lastBatchAcc + ",batchAcc=" + batchAcc);
      for (int i = lastBatchAcc; i < batchAcc; i++) { // bottleneck here
        long here1 = System.currentTimeMillis();
        if (urls.get(i).endsWith("/")) {
//          LOG.debug("Detected directory in Data content ");
//          LOG.debug("tablePaths :" + tablePaths.get(i));
          List<FileInfo> temp_infos = getDirectoryListFromHds(tablePaths.get(i), tablePaths.get(i), "");
          infos.addAll(temp_infos);
        } else {
          FileInfo info = getFileInfo(urls.get(i), tablePaths.get(i), files.get(i));
          infos.add(info); //判斷是否有資料夾
        }

        long here2 = System.currentTimeMillis();
        LOG.debug("Time difference:" + (here2 - here1));
      }
      if (batchAcc == urlSize) {
        socketGlobal.markListThreadFinished();
      }
    } catch (Exception e) {
      LOG.error("com.cdclab.yarn_test.InfoFromHDS.addFromFile() Fail:" + e.getMessage());
    }
    return infos;
  }

  ArrayList<FileInfo> addFromFileLocality(String filePath) throws IOException {
    ArrayList<FileInfo> infos = new ArrayList<>();
    Map<String, ArrayList<String>> listMap = readFileUrl(filePath);
    try {
      ArrayList<String> files = listMap.get("files");
      ArrayList<String> tablePaths = listMap.get("tablePaths");
      ArrayList<String> urls = listMap.get("urls");
      socketGlobal.setLastBatchAcc(socketGlobal.getBatchAcc());
      final int urlSize = urls.size();
//      lastBatchAcc = batchAcc;
      if (socketGlobal.getBatchAcc() + schedulerBatchSize < urlSize) {
        socketGlobal.increaseBatchAcc(schedulerBatchSize);
//        batchAcc += schedulerBatchSize;
      } else {
        socketGlobal.setBatchAcc(urlSize);
//        s.markListThreadFinished();
//        batchAcc = urls.size();
      }

      int lastBatchAcc = socketGlobal.getLastBatchAcc();
      int batchAcc = socketGlobal.getBatchAcc();

//      LOG.info("####lastBatchAcc=" + lastBatchAcc + ",batchAcc=" + batchAcc);
      for (int i = lastBatchAcc; i < batchAcc; i++) {
        if (urls.get(i).endsWith("/")) {
//          LOG.debug("Detected directory in Data content ");
//          LOG.debug("tablePaths :" + tablePaths.get(i));
          ArrayList<FileInfo> temp_infos = getDirectoryListFromHds(tablePaths.get(i), tablePaths.get(i), "");
          infos.addAll(temp_infos);
        } else {
//                    FileInfo info = getFileInfo(urls.get(i), tablePaths.get(i), files.get(i));
//                    infos.add(getFileInfo(urls.get(i), tablePaths.get(i), files.get(i))); //判斷是否有資料夾
          infos.add(getLocalityFileInfo(urls.get(i), tablePaths.get(i), files.get(i)));
        }

//                infos.add(getLocalityFileInfo(urls.get(i), tablePaths.get(i), files.get(i)));
      }

      if (batchAcc == urlSize) {
        socketGlobal.markListThreadFinished();
      }
    } catch (Exception e) {
      LOG.error("com.cdclab.yarn_test.InfoFromHDS.addFromFile() Fail:" + e.getMessage());
    }
    return infos;
  }

  private ArrayList<FileInfo> getDirectoryListFromHds(String url, String tablePath, String file) {
    ArrayList<FileInfo> infos;
    String inputUrl;
    LOG.debug("getDirectoryListFromHds");
    LOG.debug(tablePath);
    if (tablePath.length() >= 3) {
      if (tablePath.substring(0, 3).equals("hds")) {
        inputUrl = tablePath + "&limit=-1";
      } else {
        inputUrl = tablePath + "&limit=-1";
      }
    } else {
      inputUrl = tablePath + "&limit=-1";
    }
    String jsonStr = getJSON(urlTitle + inputUrl);
    infos = getDirectoryListFromJSON(jsonStr);
    return infos;
  }

  private ArrayList<FileInfo> getDirectoryListFromJSON(String JSONstr) {
    ArrayList<FileInfo> infos = new ArrayList<>();

    long size;
//        List<String> ratio = new ArrayList();
//        List<String> host = new ArrayList();
    String url;
    String file;

    try {
      JSONResult result = JsonUtils.fromJson(JSONstr, JSONResult.class);
//      LOG.debug("Datainfo size ");
//      LOG.debug("Datainfo size :" + result.getDataInfo().size());
      for (int i = 0; i < result.getDataInfo().size(); i++) {
        ArrayList<String> ratio = new ArrayList<>();
        ArrayList<String> host = new ArrayList<>();
        size = Long.parseLong(result.getDataInfo().get(i).getSize());
        file = result.getDataInfo().get(i).getName();
        url = result.getDataInfo().get(i).getUri();
//                LOG.debug(result.getDataInfo().get(i).getDataowner().size());
        for (int j = 0; j < result.getDataInfo().get(i).getDataowner().size(); j++) {
          ratio.add(result.getDataInfo().get(i).getDataowner().get(j).getRatio());
          host.add(result.getDataInfo().get(i).getDataowner().get(j).getHost());
        }
        FileInfo fileinfo = new FileInfo(url, file, size, host, ratio);
        infos.add(fileinfo);
      }
//      LOG.debug("result.getDataInfo().size()");
//      LOG.debug(result.getDataInfo().size());
    } catch (Exception e) { //com.google.gson.stream.MalformedJsonException: Unterminated string at line 1 column 1064961
      LOG.error("Building Datainfo from directory failed");
      LOG.error(e.getMessage());
    }

//    if (infos.size() == 0) {
//
//    }
    return infos;
  }

  FileInfo getFileInfo(String url, String tablePath, String file) {
    LOG.debug("Chen get File Info");
    String inputUrl;
    if (tablePath.length() >= 3) {
      if (tablePath.substring(0, 3).equals("hds")) {
        inputUrl = tablePath + "?key=" + toUrl(file);
      } else {
        inputUrl = tablePath + "?name=" + toUrl(file);
      }
    } else {
      inputUrl = tablePath + "?key=" + toUrl(file);
    }

    if (url.endsWith("/")) {
      inputUrl = tablePath;
    }

    String encodedURL = toUrl(inputUrl);
//        LOG.debug(urlTitle + encodedURL);
    String jsonStr = getJSON(urlTitle + encodedURL);
//        LOG.debug("jsonStr:" + jsonStr);

    prepareListResult(jsonStr);
    long size = getSizeFromJSON(jsonStr);

    return new FileInfo(url, file, size);

  }

  FileInfo getLocalityFileInfo(String url, String tablePath, String file) {
    String inputUrl = "";
    if (tablePath.length() >= 3) {
      if (tablePath.substring(0, 3).equals("hds")) {
        inputUrl = tablePath + "?key=" + toUrl(file);
      } else {
        inputUrl = tablePath + "?name=" + toUrl(file);
      }
    } else {
      inputUrl = tablePath + "?key=" + toUrl(file);
    }

    String encodedURL = toUrl(inputUrl);
    String jsonStr = getJSON(urlTitle + encodedURL);
    long size = getSizeFromJSON(jsonStr);
    prepareListResult(jsonStr);

    return new FileInfo(url, file, size, getHostFromJSON(jsonStr), getRatioFromJSON(jsonStr));

  }

  // The Class to Parse JSON data
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

  public void prepareListResult(String jsonStr) {
    try {
      DataInfos result = JsonUtils.fromJson(jsonStr, DataInfos.class);
    } catch (Exception e) {
      LOG.error("In prepareListResult");
      LOG.error(e.toString());
    }
  }

  public String getJSON(String url) {
    StringBuilder sb = new StringBuilder();
    url = url.concat(token);
    HttpURLConnection conn = null;
    try {
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
      LOG.error("getJSON encounter error:" + e.toString());
      try {
        LOG.error(getHdsErrorMessage(conn));
      } catch (Exception ex) {
        LOG.error("In getJSON error to getHdsErrorMessage fail");
        LOG.error(ex.getMessage());
      }
    }

    return sb.toString();
  }

  public String getHdsErrorMessage(HttpURLConnection conn) throws IOException {
    LOG.debug("In getHdsErrorMessage start");
    BufferedReader rd = null;
    StringBuilder sb = new StringBuilder("Response stream is null");
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
          sb.append(line).append('\n');
        }
      }
    } catch (Exception e) {
      LOG.error(e.toString());
      LOG.error(e.getMessage());
    }
    return sb.toString();
  }

  public List<DataInfos> getListResult() {
    return listResult;
  }

  public List<String> getHostFromJSON(String jsonStr) {
    List<String> host = new ArrayList<>();
//    LOG.debug("getHostFromJSON's jsonStr:" + jsonStr);
    try {
      JSONResult result = JsonUtils.fromJson(jsonStr, JSONResult.class);

      for (int i = 0; i < result.getDataInfo().get(0).getDataowner().size(); i++) {
        String hostname = result.getDataInfo().get(0).getDataowner().get(i).getHost();
        host.add(hostname);
      }
    } catch (Exception e) {
      LOG.error(e.toString());
      LOG.error("error in getHostFromJSON");
    }
    return host;
  }

  public List<String> getRatioFromJSON(String jsonStr) {
    List<String> ratio = new ArrayList<>();
    try {
      JSONResult result = JsonUtils.fromJson(jsonStr, JSONResult.class);
      for (int i = 0; i < result.getDataInfo().get(0).getDataowner().size(); i++) {
        ratio.add(result.getDataInfo().get(0).getDataowner().get(i).getRatio());
      }
    } catch (Exception e) {
      LOG.error(e.toString());
      LOG.error("error in getRatioFromJSON");
    }
    return ratio;
  }

  public String toUrl(String input) {
    try {
      return URLEncoder.encode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
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
