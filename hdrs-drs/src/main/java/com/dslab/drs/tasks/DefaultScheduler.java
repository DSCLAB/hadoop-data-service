package com.dslab.drs.tasks;

/**
 *
 * @author kh87313
 */
import com.dslab.drs.yarn.application.containermanager.ContainerInterval;
import com.dslab.drs.yarn.application.containermanager.ContainerManagerImpl;
import java.io.IOException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.util.List;

public class DefaultScheduler extends AbstractScheduler {

  /**
   *
   * @param csvList : 使用者輸入的CSV列表
   * @param nodeMaxCapacity :允許單一Container最大計算量
   * @throws IOException
   */
  ContainerManagerImpl containerMng = ContainerManagerImpl.getInstance();

  public DefaultScheduler(List<String> hostList, String hds_list_address, String inputFile, YarnConfiguration conf) throws IOException {
    this.hostList = hostList;
    this.hds_list_address = hds_list_address;
    this.inputFile = inputFile;
    this.conf = conf;
  }

  @Override
  public FileInfo getNewTask(String nodeName, String containerID) {
    FileInfo info = null;
    ContainerInterval cInterval
            = containerMng.getContainerInterval(containerMng.getContainerResource(containerID).getMemory());
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
      }
      LOG.debug(containerID + " get file = " + info.getName());
      if (!(cInterval.isRunning(info)
              || fileInfoStorage.failedTaskIsContain(info))) {
//                System.out.println("Find " + info.getName() + " on fileInfos"+","+info.getName());
//      } else {
        //檢查該節點最近是否有最近有在該 Container 失敗
        //如果在該節點曾經有失敗過，跳過，
        //需設置最大最大跳過次數，否則會造成這任務永遠都發不出去
        LOG.info("remove file" + info.getName());
        cInterval.addRunningFile(containerID, info);
        cInterval.rmFileInfo(i, containerID);
        fileInfoStorage.removeFileInfo(info);
        fileInfoStorage.addRunningTask(info);
//        running.add(info);
        return info;
      }

    }
    LOG.info("get new task ,but no file to do");
    return null;
  }

  public int getDisCardNumber() {
    return disCard.get();
  }

  @Override
  public void schedule() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

  }

  public class NodeInfo {

    private boolean isFull;
    private String nodeName;
    private int nodeMaxCapacity;
    private double nowCapacity;
    private List<String> nodeCsv;

  }

}
