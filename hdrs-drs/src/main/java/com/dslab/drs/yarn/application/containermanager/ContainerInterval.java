package com.dslab.drs.yarn.application.containermanager;

import com.dslab.drs.tasks.FileInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class ContainerInterval {

  private static final Log LOG = LogFactory.getLog(ContainerInterval.class);
  private final AtomicInteger upper = new AtomicInteger();
  private final AtomicInteger lower = new AtomicInteger();

  private final int memSize;
  private final AtomicInteger askForContainerNum = new AtomicInteger();
  private final AtomicInteger realTimeContainerExpectedNum = new AtomicInteger();
  private final AtomicInteger eventualContainerExpectedNum = new AtomicInteger();
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  private final List<FileInfo> files = new ArrayList<>();
  private final List<FileInfo> newFiles = new ArrayList<>();
  private final List<String> containers = new ArrayList<>();
  private final Map<String, FileInfo> runningFiles = new ConcurrentHashMap<>();
  private final Set<FileInfo> finishedFiles = new HashSet<>();

  private final List<String> intervalLog = new ArrayList<>();
  private long runTime = 0L;
  private long avgTime = 0L;
  private long thisTaskTime = 0L;
  private long taskCount = 0L;

  Map<String, Long> containerTaskTime = new ConcurrentHashMap<>();

  public ContainerInterval(int memSize, int lower, int upper, int containerExpectedNum) throws ContainerIntervalException {
    this.upper.set(upper);
    this.lower.set(lower);
    this.memSize = memSize;
    this.realTimeContainerExpectedNum.set(containerExpectedNum);
    this.eventualContainerExpectedNum.set(containerExpectedNum);
    if ((upper < lower) && (upper != -1)) {
      throw new ContainerIntervalException(memSize + "mb interval upper < lower");
    }
  }

  public void addContainer(String cID) {
    this.containers.add(cID);
  }

  public String getContainer() {
    LOG.debug(memSize + " container count = " + containers.size());
    if (containers.isEmpty()) {
      return "";
    }
    return this.containers.get(0);
  }

  public void rmContainer(String cid) {
    this.containers.remove(cid);
  }

  public synchronized void updateInterval(int containerNum, int upper, int lower) {
    this.askForContainerNum.set(memSize);
    this.upper.set(upper);
    this.lower.set(lower);
  }

  public synchronized void setUpper(int upper, boolean isOOM) {
    if (isOOM) {
      if (upper < this.upper.get()) {
        this.upper.set(upper);
      }
    } else if (upper > this.upper.get()) {
      this.upper.set(upper);
    }
  }

  public synchronized void setLower(int lower, boolean isOOM) {
    if (isOOM) {
      if (lower < this.lower.get()) {
        this.lower.set(lower);
      }
    } else if (lower > this.lower.get()) {
      this.lower.set(lower);
    }
  }

  public void addIntervalLog() {
    this.intervalLog.add(getBoundary());
  }

  public void showIntervalChange() {
    this.intervalLog.stream().map(v -> v + "->").forEachOrdered(System.out::print);
  }

  public String getBoundary() {
    return this.lower.get() + "-" + this.upper.toString().replace("-1", "-infinite");
  }

  public void increaseAskForContainerNum() {
    this.askForContainerNum.incrementAndGet();
  }

  public void increaseExpectedContaienrNum() {
    this.realTimeContainerExpectedNum.incrementAndGet();
    this.eventualContainerExpectedNum.incrementAndGet();
  }

  public void decreaseExpectedContaienrNum() {
    this.realTimeContainerExpectedNum.decrementAndGet();
    this.eventualContainerExpectedNum.decrementAndGet();
  }

  public void setAskForContainerNum(int num) {
    this.askForContainerNum.set(num);
  }

  public int getAskForContainerNum() {
    return this.askForContainerNum.get();
  }

  public void setContainerExpectedNum(int num) {
    realTimeContainerExpectedNum.set(num);
  }

  public void setEventualContainerExpectedNum(int num) {
    eventualContainerExpectedNum.set(num);
  }

  public void decreaseContainerExpectedNumNow() {
    realTimeContainerExpectedNum.decrementAndGet();
  }

  public void decreaseEventualContainerExpectedNum() {
    eventualContainerExpectedNum.decrementAndGet();
  }

  public synchronized boolean isContainerNowMoreThanOne() {
    return realTimeContainerExpectedNum.get() > 1;
  }

  public synchronized int getContainerNum() {
    return realTimeContainerExpectedNum.get();
  }

  public int getMemSize() {
    return this.memSize;
  }

  public boolean isContainerUnexpected() {
    return (askForContainerNum.get() - numCompletedContainers.get() < eventualContainerExpectedNum.get());
  }

  public void increaseCompletedContainerNum() {
    numCompletedContainers.incrementAndGet();
  }

  public int getUpperBound() {
    return this.upper.get();
  }

  public int getLowerBound() {
    return this.lower.get();
  }

  public boolean isFileBelongInterval(long fileSize) {
    if (this.upper.get() != -1) {
      return fileSize < this.upper.get() && fileSize >= this.lower.get();
    } else {
      return fileSize >= this.lower.get();
    }
  }

  public synchronized void addFileIntoInterval(FileInfo f) {
    if (isFinished(f) || isRunning(f)) {
      return;
    }
    files.add(f);
    Collections.sort(files);
  }

  public synchronized void addFileInfo(FileInfo f) {
    if (isAbsent(f)) {
      files.add(f);
      Collections.sort(files);
    }
  }

  public synchronized void addFileInfos(List<FileInfo> f) {
    newFiles.clear();
    f.stream().filter(info -> isFileBelongInterval(info.getSize()))
            .filter(info -> isAbsent(info)).forEach(info -> newFiles.add(info));
    files.addAll(newFiles);
    Collections.sort(files);
  }

  private synchronized boolean isAbsent(FileInfo f) {
    return !files.contains(f);
  }

  public synchronized int getFilesSize() {
    return files.size();
  }

  public synchronized int getRunningFilesSize() {
    return runningFiles.size();
  }

  public synchronized FileInfo getFileInfo(int index) {
    return files.get(index);
  }

  public synchronized FileInfo rmFileInfo(int index, String CID) {
    long thisTime = System.currentTimeMillis();
    long lastTime;
    if (containerTaskTime.get(CID) == null) {
      lastTime = 0L;
    } else {
      lastTime = containerTaskTime.get(CID);
    }
    thisTaskTime = thisTime - lastTime;
//    LOG.debug("thisTaskTime" + thisTaskTime);
//    LOG.debug("lastTime" + lastTime);
    if (thisTaskTime > 0 && lastTime > 0) {
      long avg = avgTime * taskCount;
      taskCount++;
      avgTime = (avg + thisTaskTime) / taskCount;
//      LOG.debug("rm avgTime = " + avgTime);
    }
    containerTaskTime.put(CID, thisTime);
    return files.remove(index);
  }

  public synchronized void rmFileInfo(List<FileInfo> f) {
    files.removeAll(f);
  }

  public synchronized List<FileInfo> getFiles() {
    return this.files;
  }

  public List<FileInfo> getLastChangeFiles() {
    return this.newFiles;
  }

  public Map<String, FileInfo> getRunningFile() {
    return runningFiles;
  }

  public FileInfo rmRunningFiles(String cID) {
    return runningFiles.remove(cID);
  }

  public void rmSuccessFile(String containerID) {
    finishedFiles.add(runningFiles.remove(containerID));
  }

  public void addRunningFile(String containerID, FileInfo f) {
    runningFiles.put(containerID, f);
  }

  public boolean isRunning(FileInfo f) {
    return runningFiles.values().stream().anyMatch((task) -> (task.equals(f)));
  }

  public boolean isFinished(FileInfo f) {
    return finishedFiles.contains(f);
  }

  public long getAVGTime() {
    return avgTime;
  }

  public long getLongestRunTime(long shortTaskTimeTmp) {
    if (realTimeContainerExpectedNum.get() == 0) {
      LOG.info("Interval has no container but needs.");
      return Integer.MAX_VALUE;
    } else {
      if (avgTime == 0) {
        if ((files.size() + runningFiles.size()) == 0) {
          runTime = -1;
        } else if (((double) (files.size() + runningFiles.size()) / (double) realTimeContainerExpectedNum.get())
                == ((files.size() + runningFiles.size()) / realTimeContainerExpectedNum.get())) {
          runTime = shortTaskTimeTmp * ((files.size() + runningFiles.size()) / realTimeContainerExpectedNum.get());
        } else {
          runTime = shortTaskTimeTmp * (((files.size() + runningFiles.size()) / realTimeContainerExpectedNum.get()) + 1);
        }
      } else if ((files.size() + runningFiles.size()) == 0) {
        runTime = -1;
      } else if (((double) (files.size() + runningFiles.size()) / (double) realTimeContainerExpectedNum.get())
              == ((files.size() + runningFiles.size()) / realTimeContainerExpectedNum.get())) {
        runTime = avgTime * ((files.size() + runningFiles.size()) / realTimeContainerExpectedNum.get());
      } else {
        runTime = avgTime * (((files.size() + runningFiles.size()) / realTimeContainerExpectedNum.get()) + 1);
      }
      return runTime;
    }
  }

  public long getPredictRunTime(long shortTaskTimeTmp, long containerCount) {
    if (containerCount > 0) {
      if (avgTime == 0) {
        if (((double) (files.size() + runningFiles.size()) / (double) containerCount)
                == ((files.size() + runningFiles.size()) / containerCount)) {
          return shortTaskTimeTmp * ((files.size() + runningFiles.size()) / containerCount);
        } else {
          return shortTaskTimeTmp * (((files.size() + runningFiles.size()) / containerCount) + 1);
        }
      } else if (((double) (files.size() + runningFiles.size()) / (double) containerCount)
              == ((files.size() + runningFiles.size()) / containerCount)) {
        return avgTime * ((files.size() + runningFiles.size()) / containerCount);
      } else {
        return avgTime * (((files.size() + runningFiles.size()) / containerCount) + 1);
      }
    } else {
      return -1;
    }
  }

}
