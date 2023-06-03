package com.dslab.drs.tasks;

import java.util.Vector;

/**
 *
 * @author chen10
 */
public class NodeProgress {

  String NodeName;
  Vector<String> readyInfos;
  Vector<String> doneInfos = new Vector<>();
  Vector<String> failInfos = new Vector<>();

  public NodeProgress(String nodeName, Vector<String> readyInfos) {
    this.readyInfos = readyInfos;
    this.NodeName = nodeName;
  }

}
