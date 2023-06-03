/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.util.Optional;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.datastorage.*;

/**
 *
 * @author brandboat
 */
public enum Protocol {
  hds((short) 0, HdsDataStorage.class.getCanonicalName()),
  hbase((short) 1, null),
  hdfs((short) 2, HdfsDataStorage.class.getCanonicalName()),
  local((short) 3, LocalDataStorage.class.getCanonicalName()),
  ftp((short) 4, FtpDataStorage.class.getCanonicalName()),
  smb((short) 5, SmbDataStorage.class.getCanonicalName()),
  file((short) 6, FileDataStorage.class.getCanonicalName()),
  http((short) 7, HttpDataStorage.class.getCanonicalName()),
  https((short) 10, HttpsDataStorage.class.getCanonicalName()),
  jdbc((short) 8, JdbcDataStorage.class.getCanonicalName()),
  sftp((short) 9, SftpDataStorage.class.getCanonicalName());

  private final byte[] code;
  private final Optional<String> className;

  Protocol(final short c, final String clzName) {
    code = Bytes.toBytes(c);
    className = Optional.ofNullable(clzName);
  }

  public byte[] getCode() {
    return code;
  }

  public Optional<String> getClassName() {
    return className;
  }

  public static Optional<Protocol> find(String protocol) {
    if (protocol == null) {
      return Optional.empty();
    }
    for (Protocol p : Protocol.values()) {
      if (p.name().equalsIgnoreCase(protocol)) {
        return Optional.of(p);
      }
    }
    return Optional.empty();
  }

  public static Optional<Protocol> find(byte[] code) {
    if (code == null) {
      return Optional.empty();
    }
    for (Protocol pro : Protocol.values()) {
      if (Bytes.compareTo(code, pro.getCode()) == 0) {
        return Optional.of(pro);
      }
    }
    return Optional.empty();
  }
}
