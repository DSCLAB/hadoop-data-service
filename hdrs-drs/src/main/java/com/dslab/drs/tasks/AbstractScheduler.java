package com.dslab.drs.tasks;

import com.dslab.drs.api.NodeStatus;
import com.dslab.drs.socket.SocketGlobalVariables;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.yarn.application.containermanager.ContainerManagerImpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 *
 * @author caca
 */
public abstract class AbstractScheduler implements Scheduler {

  protected static final Log LOG = LogFactory.getLog(AbstractScheduler.class);

  public List<String> hostList;
  protected ArrayList<FileInfo> inputFileInfos;
  public ConcurrentHashMap<String, Vector<FileInfo>> NodeProgress = new ConcurrentHashMap<>();

  protected String hds_list_address;
  protected AtomicInteger successCount = new AtomicInteger(0);
  protected AtomicInteger disCard = new AtomicInteger(0);
  protected HashSet<String> completeNodes = new HashSet<>();
  protected HashSet<String> failNodes = new HashSet<>();
  protected Long hdsFileInfoCollectTimeMs;
  protected String inputFile;
  protected YarnConfiguration conf;

  private ExecutorService listThread = Executors.newSingleThreadExecutor();

  //統計某個Task失敗總次數
  protected HashMap<String, Integer> failTimeMap = new HashMap<>();
  //紀錄該task失敗節點名

  public HashMap<String, Long> fileSizeMap = new HashMap<>();

  public HashMap<String, NodeStatus> nodesStatus = new HashMap<>();    //用containerID當 key
  protected FileInfoStorage fileInfoStorage = FileInfoStorage.getFileInfoStorage();

  protected void schedulerInit() throws Exception {

    Long startTime = System.currentTimeMillis();
    int listCount = 0;
    do {
      listCount++;
      inputFileInfos = getInputFileInfo(hds_list_address, this.inputFile);
      SocketGlobalVariables.getAMGlobalVariable().addServiceStatusTask(inputFileInfos);
      fileInfoStorage.addFileInfoList(filterEmptyFiles(inputFileInfos));
      LOG.info("list count = " + listCount);
      if (listCount == 1) {
//        LOG.info("first list finished");
        hdsFileInfoCollectTimeMs = System.currentTimeMillis() - startTime;
        SocketGlobalVariables.getAMGlobalVariable().firstListFinished();
      }
      LOG.info("Total file size =" + fileInfoStorage.getTaskAcc() + ",batch size=" + inputFileInfos.size());
//      LOG.info("Sleep60 , list count="+listCount);
    } while (!SocketGlobalVariables.getAMGlobalVariable().isListThreadFinished());
  }

  public void scheduleTasks() throws Exception {

  }

  @Override
  public Long getHdsFileInfoCollectTimeMs() {
    return this.hdsFileInfoCollectTimeMs;
  }

  @Override
  public ArrayList<FileInfo> getInputFileInfos() {
    return this.inputFileInfos;
  }

  @Override
  public void init() {
    try {
      this.listThread.submit(new ListThread());
    } catch (Exception ex) {
      LOG.error("Encounter error during scheduler init");
      ex.printStackTrace();
    }
  }

  @Override
  public void schedule() {
    try {
      scheduleTasks();
    } catch (Exception ex) {
      LOG.error("Encounter error during scheduler schedule");
      ex.printStackTrace();
    }
  }

  @Override
  public FileInfo getNewTask(String nodeName, String containerID) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isComplete() {
    return SocketGlobalVariables.getAMGlobalVariable().isListThreadFinished()
            && (fileInfoStorage.getTaskAcc() == (successCount.get() + disCard.get()));
  }

  @Override
  public boolean isListThreadRunning() {
    return !SocketGlobalVariables.getAMGlobalVariable().isListThreadFinished();
  }

  @Override
  public boolean noFreeFileAndSomeFileRunning() {
    return !hasFreeTasks()
            && fileInfoStorage.getRunningTaskSize() > 0;
  }

  @Override
  public boolean hasFreeTasks() {  //還有檔案可以或 且 還有檔案沒有發出去時
    return fileInfoStorage.getTaskSize() != 0
            || !fileInfoStorage.listDone();
//    return !fileInfoStorage.listDone() && fileInfoStorage.getTaskSize() != 0;
  }

  @Override
  public void markSuccess(String fileName, String nodeName, String containerID) {
    nodesStatus.get(containerID).setWorkingTask("");

    FileInfo info = null;
    for (int i = fileInfoStorage.getRunningTaskSize() - 1; i >= 0; i--) {
      info = fileInfoStorage.getRunningTask(i);
      if (info.getName().equals(fileName)) {
        nodesStatus.get(containerID).addDone(fileName, info.getSize());

        LOG.debug("markSuccess:" + fileName);
        completeNodes.add(fileName);
        successCount.set(completeNodes.size());
        fileInfoStorage.removeRunningTask(info);
        fileInfoStorage.removeFileInfo(info);

        //從running移除
        for (int j = fileInfoStorage.getRunningTaskSize() - 1; j >= 0; j--) {
          FileInfo temp = fileInfoStorage.getRunningTask(j);
          if (temp.getName().equals(fileName)) {
            fileInfoStorage.removeRunningTask(j);
            break;
          }
        }

        break;
      }

    }
  }

  @Override
  public void markFail(String fileName, String nodeName, String containerID, String errorMessage, boolean isPreempted) {
    LOG.debug("markFail:" + fileName);
    FileInfo info;
    for (int i = fileInfoStorage.getRunningTaskSize() - 1; i >= 0; i--) {
      info = fileInfoStorage.getRunningTask(i);
      if (info.getName().equals(fileName)) {
        LOG.debug("markFail:" + fileName);

        //如果已達最大次數，要移到FAIL ，反之移動到等待貯列。
        fileInfoStorage.removeRunningTask(info);
        int mem = ContainerManagerImpl.getInstance().getContainerResource(containerID).getMemory();
        ContainerManagerImpl.getInstance().getContainerInterval(mem).rmRunningFiles(containerID);

        nodesStatus.get(containerID).addFail(fileName + ":  " + errorMessage, info.getSize());
        nodesStatus.get(containerID).setWorkingTask("");

        if (isPreempted) {
          fileInfoStorage.addFileInfoToTail(info);
          fileInfoStorage.addFileInfoToIntervalTail(info);
          ContainerManagerImpl.getInstance().getContainerInterval(mem).addFileInfo(info);
          return;
        }

        //紀錄fail資訊
        int max_retry_times = conf.getInt(
                DrsConfiguration.CONTAINER_TASK_RETRY_TIMES,
                DrsConfiguration.CONTAINER_TASK_RETRY_TIMES_DEFAULT);

        Integer failTime = failTimeMap.get(info.getName());
        if (failTime == null) {
          failTime = 1;
        } else {
          failTime += 1;
        }
        failTimeMap.put(fileName, failTime);
        LOG.debug("failTime:" + failTime);

        if (failTime == max_retry_times) {
          LOG.debug("failTime than " + DrsConfiguration.CONTAINER_TASK_RETRY_TIMES);
          failNodes.add(fileName);
          disCard.set(failNodes.size());
          fileInfoStorage.addFailedTask(info);
//          failed.add(info);
        } else {
          fileInfoStorage.addFileInfoToTail(info);
          fileInfoStorage.addFileInfoToIntervalTail(info);
        }
        break;
      }
    }
  }

  @Override
  public int getTaskNumber() {
    return fileInfoStorage.getTaskSize();
  }

  @Override
  public int getTaskAccNumber() {
    return fileInfoStorage.getTaskAcc();
  }

  @Override
  public int getProgress() {
    //5/18 update
    return successCount.get();
  }

  @Override
  public HashMap<String, NodeStatus> getNodesStatus() {
    return this.nodesStatus;
  }

  @Override
  public List<String> getHostList() {
    return this.hostList;
  }

  protected List<FileInfo> filterEmptyFiles(List<FileInfo> inputFileInfos) {
    List<FileInfo> tempFileInfos = new ArrayList<>();
    for (FileInfo info : inputFileInfos) {
      if (info.getSize() > 0) {
        tempFileInfos.add(info);
      } else {
        info.setTaskStatus("File not found.");
      }
    }
    return tempFileInfos;
  }

  ArrayList<FileInfo> getInputFileInfo(String hds_list_address, String inputFile) throws IOException {
    InfoFromHDS infoFromHDS = new InfoFromHDS(hds_list_address,
            conf.getInt(DrsConfiguration.DRS_SCHEDULER_BATCH_SIZE, DrsConfiguration.DRS_SCHEDULER_BATCH_SIZE_DEFAULT));//"http://140.116.245.133:8000/dataservice/v1/list?from="
    infoFromHDS.setToken(conf.get(DrsConfiguration.DRS_HDS_AUTH_TOKEN, DrsConfiguration.DRS_HDS_AUTH_TOKEN_DEFAULT));
    ArrayList<FileInfo> tempInputFileInfo = getInputFileInfodiff(infoFromHDS);
    int zipNum = 0;
    for (FileInfo info : tempInputFileInfo) {
      zipNum++;
      String fileName = info.getName();
      String tableName = info.getUrl();
      long zipSize = info.getSize();
      FileInfo temp = new FileInfo(tableName, fileName, zipSize);
      fileSizeMap.put(fileName, zipSize);
    }
    return tempInputFileInfo;
  }

  /**
   * some different in method "getInputFile",cause locality and default.
   */
  protected ArrayList<FileInfo> getInputFileInfodiff(InfoFromHDS infoFromHDS) throws IOException {
    return tempInputFileInfoFrom(infoFromHDS, false);
  }

  protected ArrayList<FileInfo> tempInputFileInfoFrom(InfoFromHDS infoFromHDS, boolean isLocality) throws IOException {
    if (inputFile.endsWith("/")) {
      return infoFromHDS.addFromDirectory(inputFile);
    } else if (isLocality) {
      return infoFromHDS.addFromFileLocality(inputFile);
    } else {
      return infoFromHDS.addFromFile(inputFile);
    }
  }

  @Override
  public void closeListThread() {
    this.listThread.shutdownNow();
  }

  private class ListThread extends Thread {

    @Override
    public void run() {
      try {
        LOG.info("schedule list start");
        schedulerInit();
      } catch (Exception ex) {
        LOG.debug(ex);
      } finally {
        SocketGlobalVariables.getAMGlobalVariable().markListThreadFinished();
        LOG.info("list thread finished.");
      }
    }
  }

}
