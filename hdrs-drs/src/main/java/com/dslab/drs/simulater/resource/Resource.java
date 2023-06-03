package com.dslab.drs.simulater.resource;

import com.dslab.drs.simulater.connection.hds.HdsRequester;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class Resource {

  private static final Log LOG = LogFactory.getLog(Resource.class);

  public void copyFromFolderToHds(int fileCount, String localFolder, String remoteFolder, String fileExtension, DrsConfiguration conf) throws IOException {
    LOG.debug("Copy folder files to hds, file count:" + fileCount + ",localFolder: " + localFolder + ",remoteFolder: " + remoteFolder + ",fileExtension: " + fileExtension);
    File[] listFiles = listFiles(localFolder);

    int fileListSize = listFiles.length;
    if (fileListSize == 0) {
      throw new IOException("Copy source failed," + localFolder + " has 0 file.");
    }
    HdsRequester hdsRequest = new HdsRequester();
    for (int i = 1; i <= fileCount; i++) {
      int uploadFileNumber = (i - 1) % fileListSize;
      File uploadFile = listFiles[uploadFileNumber];
      String remotePath = remoteFolder + i + "." + fileExtension;
      LOG.debug("uploadFile:" + uploadFile.getAbsolutePath());
      LOG.debug("remotePath:" + remotePath);
      hdsRequest.uploadToHds(uploadFile.getAbsolutePath(), remotePath, conf);
    }
  }

  //未測試
  public void copyFromOneFileToHds(int fileCount, String localFile, String remoteFolder, String fileExtension, DrsConfiguration conf) throws IOException {
    LOG.debug("Copy One files to hds, file count:" + fileCount + ",localFile: " + localFile + ",remoteFolder: " + remoteFolder + ",fileExtension: " + fileExtension);
    File inputFile = new File(localFile);
    if (!inputFile.isFile()) {
      throw new IOException("Copy source failed," + localFile + " not a file.");
    }

    HdsRequester hdsRequest = new HdsRequester();
    for (int i = 1; i <= fileCount; i++) {
      //寫在本地再上傳，並複製
      File tmpFile = new File(localFile + ".tmp." + i);
      copyInputCsvFiles(inputFile, tmpFile, i / 10 + 1);
      String remotePath = remoteFolder + i + "." + fileExtension;
      LOG.debug("uploadFile:" + tmpFile.getAbsolutePath());
      LOG.debug("remotePath:" + remotePath);
      hdsRequest.uploadToHds(tmpFile.getAbsolutePath(), remotePath, conf);
      tmpFile.deleteOnExit();
    }
  }

  public void copyInputCsvFiles(File input, File output, int multiple) throws IOException {
    //先讀到記憶體
    String firstline;
    StringBuilder csvBuilder = new StringBuilder();
    String nl = System.getProperty("line.separator");

    try (FileReader fr = new FileReader(input); BufferedReader br = new BufferedReader(fr)) {
      firstline = br.readLine() + nl;
      String line;
      while ((line = br.readLine()) != null) {
        csvBuilder.append(line).append(nl);
      }
    }

    try (FileWriter fw = new FileWriter(output); BufferedWriter bw = new BufferedWriter(fw)) {
      bw.append(firstline);
      for (int i = 0; i < multiple; i++) {
        bw.append(csvBuilder);
      }
    }
  }

  private File[] listFiles(String folder) throws IOException {
    File folderFile = new File(folder);
    if (!folderFile.isDirectory()) {
      throw new IOException(folder + " not a directory");
    }
    return folderFile.listFiles();
  }
}
