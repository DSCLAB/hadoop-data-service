package com.dslab.drs.tasks;

import com.dslab.drs.yarn.application.containermanager.ContainerManagerImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public final class FileInfoStorage {

  private final List<FileInfo> fileInfoTasks = new ArrayList<>();
  private boolean listDone = false;
  private final List<FileInfo> runningTasks = new ArrayList<>();
  private final List<FileInfo> failedTasks = new ArrayList<>();
  private int taskAcc = 0;
  private final ContainerManagerImpl cMng = ContainerManagerImpl.getInstance();

  private static FileInfoStorage INSTANCE;

  protected static final Log LOG = LogFactory.getLog(FileInfoStorage.class);

  private FileInfoStorage() {
  }

  public static FileInfoStorage getFileInfoStorage() {
    if (INSTANCE == null) {
      synchronized (FileInfoStorage.class) {
        if (INSTANCE == null) {
          INSTANCE = new FileInfoStorage();
        }
      }
    }
    return INSTANCE;
  }

  /**
   * for taskList
   */
  public boolean listDone() {
    return this.listDone;
  }

  public void setListDone(boolean listDone) {
    this.listDone = listDone;
  }

  public synchronized void addFileInfoList(List<FileInfo> fileInfo) {
    fileInfoTasks.addAll(fileInfo);
    Collections.sort(fileInfoTasks);
    scheduleFileIntoContainerIntervals(fileInfo);
    taskAcc += fileInfo.size();
//    showTasksSize();
  }

  private void scheduleFileIntoContainerIntervals(List<FileInfo> list) {
    list.stream().forEach(fileInfo -> cMng.getTheFileInterval(fileInfo.getSize()).addFileIntoInterval(fileInfo));
  }

  private void showTasksSize() {
    fileInfoTasks.stream().forEach(f -> System.out.println("fileName = " + f.getName()
            + ", fileSize = " + f.getSize()));
  }

  public synchronized void addFileInfoToTail(FileInfo fileInfo) {
    fileInfoTasks.add(fileInfo);
  }

  public synchronized void addFileInfoToIntervalTail(FileInfo fileInfo) {
    cMng.getTheFileInterval(fileInfo.getSize()).addFileIntoInterval(fileInfo);
//    fileInfoTasks.add(fileInfo);
  }

  public synchronized int getTaskAcc() {
    return this.taskAcc;
  }

  public synchronized int getTaskSize() {
    return fileInfoTasks.size();
  }

  public synchronized FileInfo getMaxSizeFileInfo() {
    FileInfo fileInfo = fileInfoTasks.remove(fileInfoTasks.size() - 1);
    addRunningTask(fileInfo);
    return fileInfo;
  }

  public synchronized void rmMaxSizeFileInfo(FileInfo info) {
    fileInfoTasks.remove(info);
    addRunningTask(info);
  }

  public synchronized FileInfo getFileInfo(int index) {
    return fileInfoTasks.get(index);
  }

  public synchronized FileInfo getFileInfo(FileInfo f) {
    return fileInfoTasks.get(fileInfoTasks.indexOf(f));
  }

  public synchronized void removeFileInfo(FileInfo fileInfo) {
    fileInfoTasks.remove(fileInfo);
  }

  /**
   * for running list
   */
  public synchronized void addRunningTask(FileInfo fileInfo) {
    runningTasks.add(fileInfo);
  }

  public synchronized boolean runningTaskIsContain(FileInfo f) {
    return runningTasks.contains(f);
  }

  public synchronized int getRunningTaskSize() {
    return runningTasks.size();
  }

  public synchronized FileInfo getRunningTask(int index) {
    return runningTasks.get(index);
  }

  public synchronized void removeRunningTask(FileInfo f) {
    runningTasks.remove(f);
  }

  public synchronized void removeRunningTask(int index) {
    runningTasks.remove(index);
  }

  /**
   * for failed list
   */
  public synchronized void addFailedTask(FileInfo fileInfo) {
    failedTasks.add(fileInfo);
  }

  public synchronized boolean failedTaskIsContain(FileInfo f) {
    return failedTasks.contains(f);
  }

  public synchronized int getFailedTaskSize() {
    return failedTasks.size();
  }

  public synchronized void removeFailedTask(FileInfo f) {
    failedTasks.remove(f);
  }

}
