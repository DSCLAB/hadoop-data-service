package com.dslab.drs.tasks;

import com.dslab.drs.restful.api.response.list.DataInfos;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.yarn.application.containermanager.ContainerInterval;
import com.dslab.drs.yarn.application.containermanager.ContainerManagerImpl;
import java.util.List;

/**
 *
 * @author caca
 */
public class LocalityScheduler extends AbstractScheduler {

  List<DataInfos> localityList = new ArrayList<>();
  ContainerManagerImpl containerMng = ContainerManagerImpl.getInstance();

  public LocalityScheduler(List<String> hostList, String hds_list_address, String inputFile, YarnConfiguration conf) throws IOException {
    this.hostList = hostList;
    this.hds_list_address = hds_list_address;
    this.inputFile = inputFile;
    this.conf = conf;
  }

  @Override
  protected ArrayList<FileInfo> getInputFileInfodiff(InfoFromHDS infoFromHDS) throws IOException {
    localityList = infoFromHDS.getListResult();
    return tempInputFileInfoFrom(infoFromHDS, true);
  }

  @Override
  public FileInfo getNewTask(String nodeName, String containerID) {
    FileInfo info = null;
    ContainerInterval cInterval
            = containerMng.getContainerInterval(containerMng.getContainerResource(containerID).getMemory());
    long thresholdSize = conf.getInt(DrsConfiguration.DISPATCH_LOCALITYSCHEDULER_THRESHOLD, DrsConfiguration.DISPATCH_LOCALITYSCHEDULER_THRESHOLD_DEFAULT);
    long localityScheduleTimeout = conf.getInt(DrsConfiguration.DISPATCH_LOCALITYSCHEDULER_TIMEOUT, DrsConfiguration.DISPATCH_LOCALITYSCHEDULER_TIMEOUT_DEFAULT);
    //get fileInfo from logical containerMng,then get file form fileInfoStorage.
    int taskNum = cInterval.getFilesSize();
    LOG.debug(cInterval.getMemSize() + " -> task number =" + taskNum);
    for (int i = taskNum - 1; i >= 0; i--) {
      info = cInterval.getFileInfo(i);
      if (cInterval.isFinished(info)) {
        LOG.debug(info.getName() + " has finished.");
        continue;
      }
      if (cInterval.isRunning(info)) {
        LOG.debug(info.getName() + " is running.");
        continue;
      } else {
        cInterval.addRunningFile(containerID, info);
      }
      LOG.debug(containerID + " get file = " + info.getName());
      if (info.getSize() >= thresholdSize) {  //locality schedule occur
        Date currentDate = new Date();
        //check time out first
        if (info.isWaiting() && info.getWaitPassTime(currentDate.getTime()) > localityScheduleTimeout) {
          cInterval.rmFileInfo(i, containerID);
          fileInfoStorage.removeFileInfo(info);
          fileInfoStorage.addRunningTask(info);
          return info;
        }
        if (checkLocality(info, nodeName)) {
          cInterval.rmFileInfo(i, containerID);
          fileInfoStorage.removeFileInfo(info);
          fileInfoStorage.addRunningTask(info);
          return info;
        }
        if (!info.isWaiting()) {
          info.setWaitTime(currentDate.getTime());
        }
      } else {
        cInterval.rmFileInfo(i, containerID);
        fileInfoStorage.removeFileInfo(info);
        fileInfoStorage.addRunningTask(info);
        return info;
      }
      if (i == 0) { //all are waiting or last task
        cInterval.rmFileInfo(i, containerID);
        fileInfoStorage.rmMaxSizeFileInfo(info);
        LOG.debug("i=0,info = " + info.getName());
        return info;
      }
    }
    LOG.info("get new task ,but no file to do");
    return null;
  }

//  private FileInfo findIndexofFirstNotRunningFileInfo(String nodeName, String containerID) {
//    FileInfo info = null;
//    for (int i = fileInfoStorage.getTaskSize() - 1; i >= 0; i--) {
//      info = fileInfoStorage.getMaxSizeFileInfo();
//      if (fileInfoStorage.runningTaskIsContain(info)
//              || fileInfoStorage.failedTaskIsContain(info)) {
//      } else {
//        LOG.debug("LocalityScheduler FIFO get NewTask: " + info.getName() + " for nodename:" + nodeName + " container:" + containerID);
//        fileInfoStorage.addRunningTask(info);
//        return info;
//      }
//    }
//    LOG.debug("#### all node in running or failed!!");
//    return info;
//  }
  private boolean checkLocality(FileInfo currentInfo, String nodeName) {
    Double acceptRatioOfFileSize = conf.getDouble(DrsConfiguration.DISPATCH_LOCALITY_RATIO, DrsConfiguration.DISPATCH_LOCALITY_RATIO_DEFAULT);
    boolean ok = currentInfo.isAcceptRatioOfHost(nodeName, acceptRatioOfFileSize);
    if (ok == false) {
      nodeName = checkHostNameFromFQDN(nodeName);
      ok = currentInfo.isAcceptRatioOfHost(nodeName, acceptRatioOfFileSize);
//      System.out.println("Try hostname wih FQDN :" + nodeName + " result:" + ok);
    }
    if (ok == false) {
      nodeName = checkHostNameFromFQDN(nodeName);
      ok = currentInfo.isAcceptRatioOfHostByFQDN(nodeName, acceptRatioOfFileSize);
//      System.out.println("Retry non FQDN from hostlist and hostname with :" + nodeName + " result:" + ok);
    }
    return ok;
  }

  public String checkHostNameFromFQDN(String fqdn) {
    if (fqdn == null) {
      throw new IllegalArgumentException("fqdn is null");
    }
    int dotPos = fqdn.indexOf('.');
    if (dotPos == -1) {
      return fqdn;
    } else {
      return fqdn.substring(0, dotPos);
    }
  }
}
