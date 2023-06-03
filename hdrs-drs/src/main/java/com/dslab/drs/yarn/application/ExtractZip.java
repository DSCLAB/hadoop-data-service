package com.dslab.drs.yarn.application;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 * @author caca
 */
public class ExtractZip {

  /**
   *      * Size of the buffer to read/write data      
   */
  private static final int BUFFER_SIZE = 4096;

  /**
   *       Extracts a zip file specified by the zipFilePath to a directory
   * specified by       destDirectory (will be created if does not exists)      
   *
   * @param zipFilePath      
   * @param destDirectory      
   * @throws IOException      
   */
  public static ArrayList<String> unzip(String zipFilePath, String destDirectory) throws IOException {
    ArrayList<String> Contents_File_Name = new ArrayList<>();
    int ContentsCount = 0;
    File destDir = new File(destDirectory);
    if (!destDir.exists()) {
      destDir.mkdir();
    }

    try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath))) {
      ZipEntry entry = zipIn.getNextEntry();
      // iterates over entries in the zip file
      while (entry != null) {
        String filePath = destDirectory + File.separator + entry.getName();
        if (!entry.isDirectory()) {
          // if the entry is a file, extracts it
          extractFile(zipIn, filePath);
        } else {
          // if the entry is a directory, make the directory
          File dir = new File(filePath);
          dir.mkdir();
        }
        zipIn.closeEntry();
        Contents_File_Name.add(entry.getName());
        ContentsCount++;
        entry = zipIn.getNextEntry();
      }
    }
    return Contents_File_Name;
  }

  /**
   *      * Extracts a zip entry (file entry)      * @param zipIn      
   *
   *
   * @param filePath      * @throws IOException      
   */
  private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
      byte[] bytesIn = new byte[BUFFER_SIZE];
      int read = 0;
      while ((read = zipIn.read(bytesIn)) != -1) {
        bos.write(bytesIn, 0, read);
      }
      bos.close();
    }
  }

}
