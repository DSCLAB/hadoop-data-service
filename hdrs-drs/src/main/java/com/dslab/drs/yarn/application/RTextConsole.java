package com.dslab.drs.yarn.application;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rosuda.JRI.RMainLoopCallbacks;
import org.rosuda.JRI.Rengine;

/**
 *
 * @author caca
 */
public class RTextConsole implements RMainLoopCallbacks {
  private static final Log LOG = LogFactory.getLog(RTextConsole.class);
  private String path;

  public RTextConsole() {

  }

  public RTextConsole(String path) {
    this.path = path;
  }

  //called when R prints output to the console
  public void rWriteConsole(Rengine re, String text, int oType) {

    if (path != null) {
      try {
        File file = new File(path);// 建立檔案，準備寫檔
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "utf8"));// 設定為BIG5格式
// 參數append代表要不要繼續許入該檔案中
        writer.write(text); // 寫入該字串
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("catching console!!");
      }
    }
  }

  public void rBusy(Rengine re, int which) {
    LOG.debug("rBusy(" + which + ")");
  }

  //called when R waits for user input
  public String rReadConsole(Rengine re, String prompt, int addToHistory) {
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String s = br.readLine();
      return (s == null || s.length() == 0) ? s : s + "\n";
    } catch (Exception e) {
      LOG.error("jriReadConsole exception: " + e.getMessage());
    }
    return null;
  }

  public void rShowMessage(Rengine re, String message) {
    LOG.debug("rShowMessage show a warning/error message \"" + message + "\"");
  }

  public String rChooseFile(Rengine re, int newFile) {

    String res = null;

    return res;
  }

  public void rFlushConsole(Rengine re) {
  }

  public void rLoadHistory(Rengine re, String filename) {
  }

  public void rSaveHistory(Rengine re, String filename) {
  }

  public void setConsolePath(String filepath) {
    this.path = filepath;
  }

}
