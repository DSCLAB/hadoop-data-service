package com.dslab.drs.simulater.connection.hds;

import com.dslab.drs.hdsclient.HdsClient;
import com.dslab.drs.simulater.DrsArguments;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author kh87313
 */
public class HdsRequester {

  private static final Log LOG = LogFactory.getLog(HdsRequester.class);

  //各個Tread 會利用這個物件產生各自的Drs Request.
  public String requestDrs(DrsArguments drsArguments, DrsConfiguration conf, boolean async) throws MalformedURLException, IOException {
    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS));
    hdsClient.setConf(conf);

//drsArguments.setConfig("hdfs:///tmp/drs/yarn-dispatch.xml");  
    URL runUrl = hdsClient.createDrsRunUrl(drsArguments, conf, async);

    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = (HttpURLConnection) runUrl.openConnection();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          sb.append(inputLine);
        }
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return sb.toString();
  }

  public String killDrs(String applicationId, DrsConfiguration conf) throws IOException {
    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS));
    hdsClient.setConf(conf);
    URL killUrl = hdsClient.createDrsKillUrl(applicationId, conf);
    LOG.debug("killUrl:" + killUrl);
    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = (HttpURLConnection) killUrl.openConnection();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          sb.append(inputLine);
        }
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return sb.toString();
  }

  public String watchDrs(String applicationId, Configuration conf) throws IOException {
    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_WATCH_DRS_ADDRESS));
    hdsClient.setConf(conf);
    URL drsWatchUrl = hdsClient.createDrsWatchUrl(applicationId, conf);

    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = (HttpURLConnection) drsWatchUrl.openConnection();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          sb.append(inputLine);
        }
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }

    return sb.toString();
  }

  public String list(DrsConfiguration conf, String path) throws IOException {

    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_WATCH_DRS_ADDRESS));
    hdsClient.setConf(conf);
    URL listUrl = hdsClient.createListUrl(path, conf);
    LOG.debug("list url : " + listUrl.toString());

    StringBuilder sb = new StringBuilder();
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) listUrl.openConnection();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          sb.append(inputLine);
        }
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }

    return sb.toString();
  }

  //發送HDS access url，並判斷是否需要retry
  public boolean uploadToHds(String local, String to, DrsConfiguration conf) throws MalformedURLException, IOException {
    return uploadToHds(local, to, conf, 1);
  }

  public boolean uploadToHds(String local, String to, DrsConfiguration conf, int retryTimes) throws MalformedURLException, IOException {
    for (int i = 1; i <= retryTimes; i++) {
      HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT));
      hdsClient.setConf(conf);
      URL uploadUrl = hdsClient.createUpdloadUrl(local, to, conf);
      URL retryUrl = null;
      int status = -1;
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) uploadUrl.openConnection();
        conn = hdsClient.preparePost(conn, new File(local));
        conn.connect();
        try (FileInputStream fin = new FileInputStream(local); BufferedOutputStream bw = new BufferedOutputStream(conn.getOutputStream())) {
          byte[] buffer = new byte[1024 * 10];
          int idx = 0;
          while ((idx = fin.read(buffer)) != -1) {
            bw.write(buffer, 0, idx);
            bw.flush();
          }
        }
      } catch (IOException ex) {
        LOG.info(local + "IOException :" + ex.getMessage());
        if (!ex.getMessage().startsWith("Error writing request body to serve")) {  //非 redirect 訊息
          ex.printStackTrace();
          throw ex;
        }
        //這裡面有個case java.io.IOException: Error writing request body to server
        //在HDS發送redirect訊息時，會出現(redirect 會再寫了60次後發生) 
        //該如何判斷會比較漂亮  
        //在IOException中挑出此錯誤不處理嗎?
      } finally {
        if (conn != null) {
          status = conn.getResponseCode();
          if (hdsClient.checkRedirect(status)) {
            String retryHeaderField = conn.getHeaderField("Location");
            retryUrl = hdsClient.createRetryUrl(retryHeaderField, conf);
          }
          conn.disconnect();
        }
      }

      if (retryUrl != null) {
        boolean isRetrySusccesed = retrytUpload(local, to, conf, retryUrl);
        if (isRetrySusccesed) {
          return true;
        } else if (i == retryTimes) {
          delete(to, conf);
          return false;
        } else {
          continue;
        }
      }

      if (status == HttpURLConnection.HTTP_OK) {
        return true;
      }

      if (i == retryTimes) {
        delete(to, conf);
        return false;
      } else {
        continue;
      }
    }
    delete(to, conf);
    return false;
  }

  public List<String> uploadFolderFilesToHds(String localFolder, String to, DrsConfiguration conf)
          throws FileNotFoundException, IOException {

    List<String> successfulFiles = new ArrayList<>();
    List<String> failFiles = new ArrayList<>();

    File directory = new File(localFolder);
    if (directory.listFiles().length == 0) {
      LOG.info(localFolder + "  no file to upload to " + to);
      return successfulFiles;
    }

    for (File file : directory.listFiles()) {
      String localPath = file.getAbsolutePath();
      LOG.info("upload " + localPath + " to " + to);
      String targetPath = mergePath(to, file.getName());
      if (uploadToHds(localPath, targetPath, conf, 3)) {
        successfulFiles.add(file.getName());
      } else {
        failFiles.add(file.getName());
      }
    }

    if (failFiles.size() > 0) {
      StringBuilder failMessage = new StringBuilder();
      failMessage.append("Upload folder file failed,")
              .append(successfulFiles.size()).append(" successed, ")
              .append(failFiles.size()).append(" failed.");
      throw new IOException(failMessage.toString());
    }

    return successfulFiles;
  }

  public String mergePath(String folder, String fileName) {
    String result = folder;
    if (!result.endsWith("/")) {
      result = result + "/";
    }
    return result.concat(fileName);
  }

  private boolean retrytUpload(String local, String to, DrsConfiguration conf, URL retryUrl) throws IOException {
    LOG.info("upload " + local + " to " + to + " redirect " + retryUrl.toString());

    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT));
    hdsClient.setConf(conf);
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) retryUrl.openConnection();
      conn = hdsClient.preparePost(conn, new File(local));
      conn.connect();
      try (FileInputStream fin = new FileInputStream(local); BufferedOutputStream bos = new BufferedOutputStream(conn.getOutputStream())) {
        byte[] buffer = new byte[1024 * 10];
        int idx = 0;
        while ((idx = fin.read(buffer)) != -1) {
          bos.write(buffer, 0, idx);
          bos.flush();
        }
      }
    } finally {
      if (conn != null) {
        int responseCode = conn.getResponseCode();
        String msg = conn.getResponseMessage();
        conn.disconnect();

        if (responseCode == HttpURLConnection.HTTP_OK) {
          return true;
        } else {
          LOG.info("Retry upload failed, code " + responseCode
                  + ",message " + msg);
          return false;
        }
      }
      LOG.info("Retry upload failed, conn is null");
      return false;
    }
  }

  public void delete(String target, DrsConfiguration conf) throws IOException {
    LOG.info("Delete " + target);
    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT));
    hdsClient.setConf(conf);
    URL deleteUrl = hdsClient.createDeleteUrl(target, conf);

    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) deleteUrl.openConnection();
      conn.getResponseCode();
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  public void access(String fromUrl, String toUrl, DrsConfiguration conf) throws IOException {
    if (!toUrl.startsWith("hdfs")) {
      toUrl = "hdfs://" + toUrl;
    }
    HdsClient hdsClient = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT));
    hdsClient.setConf(conf);
    URL accessUrl = hdsClient.createAccessUrl(fromUrl, toUrl, conf);

    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) accessUrl.openConnection();
      conn.getResponseCode();
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }

  }
}
