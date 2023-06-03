package com.dslab.drs.tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author kh87313
 */
public class FileInfo implements Comparable, java.io.Serializable {
  private List<String> host = new ArrayList<>();
  private List<String> ratio = new ArrayList<>();

  protected String url;
  protected String name;
  protected long size;
  private long waitTimeInSec = 0;
  private long waitTimeRecord = 0;
  private String taskStatus;

  private int failTimes = 0;

  public FileInfo(String name, long size) {
    this.name = name;
    this.size = size;
  }

  public FileInfo(String url, String name, long size) {
    this.url = url;
    this.name = name;
    this.size = size;
  }

  public FileInfo(String url, String name, long size, List<String> host, List<String> ratio) {
    this.url = url;
    this.name = name;
    this.size = size;
    this.host = host;
    this.ratio = ratio;
  }

  public long getWaitPassTime(long time) {
    waitTimeInSec = waitTimeRecord - time;
    return this.waitTimeInSec;
  }

  public String getUrl() {
    return this.url;
  }

  public String getName() {
    return this.name;
  }

  public long getSize() {
    return this.size;
  }

  public String getTaskStatus() {
    return this.taskStatus;
  }

  public void setWaitTime(long time) {
    waitTimeRecord = time;
  }

  public void setTaskStatus(String taskStatus) {
    this.taskStatus = taskStatus;
  }

  public int getFailTimes() {
    return this.failTimes;
  }

  public boolean isWaiting() {
    return this.waitTimeRecord != 0;
  }

  public boolean isAcceptRatioOfHost(String hostName, Double acceptRatioOfFileSize) {
    int index = this.host.indexOf(hostName);
    if (index != -1) {
      Double ratio = Double.parseDouble(this.ratio.get(index));
      if (ratio >= acceptRatioOfFileSize) {
        return true;
      } else {
        return false;
      }
    } else { //doesn't find it at all
      return false;
    }
  }

  public boolean isAcceptRatioOfHostByFQDN(String hostName, Double acceptRatioOfFileSize) {
    List<String> tempHostList = new ArrayList<>();
    this.host.stream().map((content) -> checkHostNameFromFQDN(content))
            .forEach((content) -> tempHostList.add(content));
    int index = tempHostList.indexOf(hostName);
    if (index != -1) {
      Double ratio = Double.parseDouble(this.ratio.get(index));
      if (ratio >= acceptRatioOfFileSize) {
        return true;
      } else {
        return false;
      }
    } else { //doesn't find it at all
      return false;
    }
  }

  public String checkHostNameFromFQDN(String fqdn) {
    if (fqdn == null) {
      throw new IllegalArgumentException("fqdn is null In FileInfo");
    }
    int dotPos = fqdn.indexOf('.');
    if (dotPos == -1) {
      return fqdn;
    } else {
      return fqdn.substring(0, dotPos);
    }
  }

  public void increaseFailTimes() {
    this.failTimes++;
  }

  public List<String> getHostList() {
    return host;
  }

  public List<String> getRatioList() {
    return ratio;
  }

  public void setHostList(List<String> host) {
    this.host = host;
  }

  public void setRatioList(List<String> ratio) {
    this.ratio = ratio;
  }

  public void sortRatioList() {
    List<Double> tempRatio = new ArrayList<>();
    for (String content : ratio) {
      Double tempdouble = Double.valueOf(content);
      tempRatio.add(tempdouble);
      Collections.sort(tempRatio);
    }
//    List<String> ratio = tempRatio.stream().map(String::valueOf).collect(Collectors.toList());
//    for (Double content : tempRatio) {
//      ratio.add(String.valueOf(content));
//    }
    this.ratio = tempRatio.stream().map(String::valueOf).collect(Collectors.toList());
  }

  @Override
  public int compareTo(Object o) {
    FileInfo fileInfo = (FileInfo) o;
    if (fileInfo.getSize() > this.size) {
      return -1;
    } else if (fileInfo.getSize() < this.size) {
      return 1;
    } else {
      return 0;
    }

  }

}
