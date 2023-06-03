/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.core;

import java.io.File;

/**
 *
 * @author Weli
 */
public class DeleteLocalFile {

  final File localFilePath;

  public DeleteLocalFile(String localFilePath) {
    this.localFilePath = new File(localFilePath);
  }

  public void deleteAll() {
    if (!localFilePath.exists()) {
      return;
    }
    if (localFilePath.isFile()) {
      localFilePath.delete();
      return;
    }
    File[] files = localFilePath.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
    localFilePath.delete();
  }

}
