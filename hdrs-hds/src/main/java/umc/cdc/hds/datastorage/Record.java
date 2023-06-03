/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;

/**
 *
 * @author brandboat
 */
public class Record {

  private final String key;
  private final long size;
  private final long uploadTime;
  private final String name;
  private final Protocol location;
  private final byte[] value;
  private final String link;

  Record(Result r) throws IOException {
    if (r.isEmpty()) {
      throw new IOException("Not found of data by key: "
          + Bytes.toString(r.getRow()));
    }
    final Optional<Protocol> l
        = Protocol.find(r.getValue(HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_LOCATION_QUALIFIER));
    if (!l.isPresent()) {
      throw new IOException("Failed to find location parameter");
    }
    byte[] s = r.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.DEFAULT_SIZE_QUALIFIER);
    if (s == null) {
      throw new IOException("Failed to find size parameter");
    }
    byte[] n = r.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.DEFAULT_NAME_QUALIFIER);
    if (n == null) {
      throw new IOException("Failed to find name parameter");
    }
    byte[] t = r.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER);
    if (t == null) {
      throw new IOException("Failed to find timestamp parameter");
    }
    byte[] v = r.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.DEFAULT_VALUE_QUALIFIER);
    byte[] k = r.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.DEFAULT_LINK_QUALIFIER);
    if (v == null && k == null) {
      throw new IOException("Both value and link parameter are empty.");
    }
    key = Bytes.toString(r.getRow());
    location = l.get();
    size = Bytes.toLong(s);
    uploadTime = Bytes.toLong(t);
    name = Bytes.toString(n);
    value = v;
    link = Bytes.toString(k);
  }

  public long getSize() {
    return size;
  }

  public long getUploadTime() {
    return uploadTime;
  }

  public String getName() {
    return name;
  }

  public Protocol getLocation() {
    return location;
  }

  public byte[] getValue() {
    return value;
  }

  public String getKey() {
    return key;
  }

  public String getLink() {
    return link;
  }
}
