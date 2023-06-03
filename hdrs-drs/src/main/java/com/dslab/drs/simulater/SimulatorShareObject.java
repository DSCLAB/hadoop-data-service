package com.dslab.drs.simulater;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author kh87313
 */
public class SimulatorShareObject {
  //總時間，上次log最後時間  /以 container為單位

  private static final Map<String, Integer> threadName_FileNumberMap = new HashMap<String, Integer>();


  public static void addTotalThreadInputFileNumber(String ThreadId, int number) {
    threadName_FileNumberMap.put(ThreadId, number);
  }

  public static int getTotalThreadInputFileNumber() {
    int totalThreadInputFileNumber = 0;
    for (Integer number : threadName_FileNumberMap.values()) {
      totalThreadInputFileNumber += number;
    }
    return totalThreadInputFileNumber;
  }

}
