/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.createFile;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author Weli
 */
public class LocalFile implements BaseFile {

  Iterator<Integer> fileSizeQueue;

  public LocalFile(Iterator<Integer> fileSizeQueue) {
    this.fileSizeQueue = fileSizeQueue;
  }

  @Override
  public void create(String resource, int i) throws IOException {
    String prefixName;
    if (resource.endsWith("/")) {
      prefixName = "Test";
    } else {
      prefixName = "/Test";
    }
    String ID = resource + prefixName + i;
    try (FileOutputStream fos = new FileOutputStream(ID)) {
      int byteSize = fileSizeQueue.next() * 1024;
      byte[] buffer = new byte[byteSize];
      fos.write(buffer);
      fos.close();
    } catch (IOException e) {
      System.out.println(e);
    }
  }

}
