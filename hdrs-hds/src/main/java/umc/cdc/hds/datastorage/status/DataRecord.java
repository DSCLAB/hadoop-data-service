/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status;

import java.util.Map;
import java.util.Optional;
import umc.cdc.hds.core.Protocol;

/**
 *
 * @author brandboat
 */
public class DataRecord {

  private final String uri;
  private final Protocol location;
  private final String name;
  private final long size;
  private final long uploadTime;
  private final Map<String, Double> dataOwner;
  private final Optional<FileType> fileType;

  public static enum FileType {
    file,
    directory
  }

  public DataRecord(String uri, Protocol location, String name, long size,
      long uploadTime, FileType fileType, Map<String, Double> dataOwner) {
    this.uri = uri;
    this.location = location;
    this.name = name;
    this.size = size;
    this.uploadTime = uploadTime;
    this.fileType = Optional.ofNullable(fileType);
    this.dataOwner = dataOwner;
  }

  public String getUri() {
    return uri;
  }

  public Protocol getLocation() {
    return location;
  }

  public long getSize() {
    return size;
  }

  public long getTime() {
    return uploadTime;
  }

  public String getName() {
    return name;
  }

  public Optional<FileType> getFileType() {
    return fileType;
  }

  public Map<String, Double> getDataOwner() {
    return dataOwner;
  }

}
