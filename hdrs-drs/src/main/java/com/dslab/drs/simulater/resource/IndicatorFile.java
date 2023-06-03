package com.dslab.drs.simulater.resource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class IndicatorFile {
  private static final Log LOG = LogFactory.getLog(IndicatorFile.class);

  public IndicatorFile() {

  }

  public void copy(String source, String target, long Size) throws IOException {
    String header;
    List<String> frontStrings = new ArrayList<>();
    List<String> endStrings = new ArrayList<>();
    String nl = System.getProperty("line.separator");
    long sourceSize = new File(source).length();
    if (sourceSize == 0) {
      throw new IOException(source + "size is zero");
    }
    long times = Size / sourceSize;
    LOG.info("Size:" + sourceSize + "  to  " + Size);
    LOG.info("Copy times:" + times);
    try (FileReader fr = new FileReader(source); BufferedReader br = new BufferedReader(fr)) {
      header = br.readLine();
      String[] headerAtributes = header.split(",");
      String line;
      while ((line = br.readLine()) != null) {
        String[] atributes = line.split(",");
        if (atributes.length > 3) {
          String frontString = atributes[0] + ",";
          StringBuilder endString = new StringBuilder(".00");
          for (int i = 2; i < atributes.length; i++) {
            endString.append(",").append(atributes[i]);
          }
          frontStrings.add(frontString);
          endStrings.add(endString.toString());
        }
      }
    }

    try (FileWriter fw = new FileWriter(target); BufferedWriter bw = new BufferedWriter(fw)) {
      fw.write(header + nl);
      int count = 1;
      for (int i = 0; i < times; i++) {
        for (int lineNumber = 0; lineNumber < frontStrings.size(); lineNumber++) {
          String line = frontStrings.get(lineNumber) + String.valueOf(count) + endStrings.get(lineNumber) + nl;
          bw.write(line);
          count++;
        }
      }
    }
  }

}
