package com.dslab.drs.yarn.application;

/**
 *
 * @author CDCLAB
 */
public class ServerToClientMessage implements java.io.Serializable {

  private String status;
  private String url;
  private String fileName;
  private long fileSize;
  private int progress;

  public ServerToClientMessage(String status, String url, String fileName, long fileSize) {
    this.status = status;
    this.url = url;
    this.fileName = fileName;
    this.fileSize = fileSize;
  }

  public ServerToClientMessage(String status) {
    this.status = status;
  }

  public ServerToClientMessage(String status, int progress) {
    this.status = status;
    this.progress = progress;
  }

  public String getStatus() {
    return this.status;
  }

  public String geUrl() {
    return this.url;
  }

  public String getFileName() {
    return this.fileName;
  }

  public int getProgress() {
    return this.progress;
  }

  public long getFileSize() {
    return this.fileSize;
  }
}
