/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author brandboat
 */
public class HdfsAuth extends AbstractAuth {

  private long modifyTime = 0L;

  public HdfsAuth(Configuration conf) throws IOException {
    super(conf);
  }

  @Override
  protected AuthInventory loadAuth(Path path) throws IOException {
    FileSystem hdfs = FileSystem.get(conf);
    try (FSDataInputStream is = hdfs.open(path)) {
      return new AuthInventory(is);
    }
  }

  @Override
  protected boolean authExisted(Path path) throws IOException {
    FileSystem hdfs = FileSystem.get(conf);
    return hdfs.exists(path);
  }

  @Override
  protected boolean authChanged(Path path) throws IOException {
    FileSystem hdfs = FileSystem.get(conf);
    if (authExisted(path)) {
      FileStatus fs = hdfs.getFileStatus(path);
      long fmodify = fs.getModificationTime();
      boolean changed = modifyTime != fmodify;
      modifyTime = fmodify;
      return changed;
    }
    return false;
  }
}
