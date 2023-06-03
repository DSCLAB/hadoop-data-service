/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.auth;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author brandboat
 */
public class LocalAuth extends AbstractAuth {

  private long modifyTime = 0L;

  public LocalAuth(Configuration conf) throws IOException {
    super(conf);
  }

  @Override
  protected AuthInventory loadAuth(Path path) throws IOException {
    File file = new File(path.toString());
    try (FileInputStream is = new FileInputStream(file)) {
      return new AuthInventory(is);
    }
  }

  @Override
  protected boolean authExisted(Path path) throws IOException {
    File file = new File(path.toString());
    return file.exists();
  }

  @Override
  protected boolean authChanged(Path path) throws IOException {
    File file = new File(path.toString());
    long fmodify = file.lastModified();
    boolean changed = modifyTime != fmodify;
    modifyTime = fmodify;
    return changed;
  }

}
