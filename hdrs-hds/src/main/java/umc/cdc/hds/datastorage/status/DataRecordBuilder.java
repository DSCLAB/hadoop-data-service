/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status;

import java.util.Map;
import java.util.TreeMap;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;
import umc.cdc.hds.exceptions.ListElementException;

/**
 *
 * @author jpopaholic
 */
public class DataRecordBuilder {

  private String uri;
  private Protocol location;
  private String name;
  private Long size;
  private Long uploadTime;
  private Map<String, Double> dataOwner;
  private FileType fileType;

  public DataRecordBuilder() {
    dataOwner = new TreeMap();
  }

  public DataRecordBuilder setUri(String uri) {
    this.uri = uri;
    return this;
  }

  public DataRecordBuilder setProtocol(Protocol pol) {
    this.location = pol;
    return this;
  }

  public DataRecordBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public DataRecordBuilder setSize(long size) {
    this.size = size;
    return this;
  }

  public DataRecordBuilder setUploadTime(long uploadTime) {
    this.uploadTime = uploadTime;
    return this;
  }

  public DataRecordBuilder setOwner(Map<String, Double> dataOwner) {
    this.dataOwner = dataOwner;
    return this;
  }

  public DataRecordBuilder setFileType(FileType type) {
    this.fileType = type;
    return this;
  }

  public DataRecord build() throws RuntimeException {
    if (uri != null
        && location != null
        && name != null
        && size != null
        && uploadTime != null) {
      return new DataRecord(uri,
          location,
          name,
          size,
          uploadTime,
          fileType,
          dataOwner);
    }
    throw new ListElementException(HDSConstants.LIST_ELEMENT_MISSING_ARG);
  }

  public DataRecordBuilder clean() {
    uri = null;
    location = null;
    name = null;
    size = null;
    uploadTime = null;
    fileType = null;
    dataOwner.clear();
    fileType = null;
    return this;
  }
}
