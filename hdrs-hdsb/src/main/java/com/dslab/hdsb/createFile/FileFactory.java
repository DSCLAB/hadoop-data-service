/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.createFile;

import com.dslab.hdsb.distlib.DistlabModel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 *
 * @author Weli
 */
public class FileFactory {

  Iterator<Integer> fileSizeQueue;

  public FileFactory(Iterator<Integer> fileSizeQueue) {
    this.fileSizeQueue = fileSizeQueue;
  }

  public enum FILETYPE {
    FTP, LOCAL, SAMBA, FILE;
  }

  public BaseFile createFile(FILETYPE fileType) {
    switch (fileType) {
      case FTP:
      case LOCAL:
        return new LocalFile(fileSizeQueue);
      case SAMBA:
      case FILE:
        return new LocalFile(fileSizeQueue);

      default:
        System.out.println("No suit File Type");
    }
    return null;
  }

}
