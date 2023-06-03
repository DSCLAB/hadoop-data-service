package com.dslab.drs.socket;

import com.dslab.drs.api.NodeStatus;
import com.dslab.drs.tasks.FileInfo;
import com.dslab.drs.yarn.application.ServerToClientMessage;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class FailTask extends ContainerResponseState {

  private static final Log LOG = LogFactory.getLog(ContainerResponseState.class);
  private final SocketRun sr;
  private final SocketGlobalVariables globals;

  FailTask(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {

    String fileUrl = sr.getContainerTaskStatus().getTaskUrl();
    String fileName = sr.getAsk()[3];
    String nodeName = sr.getAsk()[1];
    String containerID = sr.getAsk()[2];
    int taskSize = Integer.valueOf(sr.getAsk()[4]);
    String errorMessage = sr.getContainerTaskStatus().getErrorMessage();
    List<String> outputfiles = sr.getContainerTaskStatus().getOutputFiles();
    globals.setServiceTStatusTaskStatus(fileUrl, "Failed", outputfiles);
    globals.getScheduler().markFail(fileName, nodeName, containerID, errorMessage, false);
    globals.logContainerTime(containerID, System.currentTimeMillis());//更新最後更新時間
    //container resize
    if (sr.getContainerTaskStatus().getProcMemInfo().getMem() != 0) {
      globals.getContainerMng().updateIntervals(taskSize, sr.getContainerTaskStatus().getProcMemInfo());
    }

    try {
      if (globals.getScheduler().isComplete()
              || globals.getScheduler().noFreeFileAndSomeFileRunning()) {
        LOG.debug("Ask for " + containerID + " to Finish.");
        LOG.info("containerID:" + containerID + " finish -> close!");
//        globals.decreaseAMExpectedContainer();
        globals.addReleasedContainerID(containerID);
        ServerToClientMessage message = new ServerToClientMessage("Finish");
        outputMessage(message, sr.getSocket());

      } else {

        FileInfo info = globals.getScheduler().getNewTask(nodeName, containerID);

        if (info == null) {
          ServerToClientMessage message = new ServerToClientMessage("Wait");
          outputMessage(message, sr.getSocket());
        } else {
          globals.getScheduler().getNodesStatus().get(containerID).setWorkingTask(info.getName());
          globals.containerRunningTaskMark(containerID, info.getName(), info.getSize());
          String url = info.getUrl();
          String toClientFileName = info.getName();
          long fileSize = info.getSize();
          ServerToClientMessage message = new ServerToClientMessage("NewTask", url, toClientFileName, fileSize);
          outputMessage(message, sr.getSocket());

          LOG.debug("Tell Client :NewTask " + info.getUrl() + " " + info.getName());
        }
      }
    } catch (IOException ex) {
    } finally {
      int memory = globals.getContainerMng().getContainerResource(containerID).getMemory();
      String interval = globals.getContainerMng().getContainerInterval(memory).getBoundary();
      globals.getScheduler().getNodesStatus().keySet().stream()
              .filter(k -> globals.getScheduler().getNodesStatus().get(k).isSameInterval(memory))
              .forEach(k -> globals.getScheduler().getNodesStatus().get(k).setContainerInterval(interval));
      globals.getServiceStatus().updateNodesStatus(globals.getScheduler().getNodesStatus(), globals.getScheduler().getProgress());
    }
  }

}
