package com.dslab.drs.socket;

import com.dslab.drs.tasks.FileInfo;
import com.dslab.drs.yarn.application.ServerToClientMessage;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NewTask extends ContainerResponseState {

  private static final Log LOG = LogFactory.getLog(NewTask.class);
  private final SocketRun sr;
  private final SocketGlobalVariables globals;

  NewTask(SocketRun sr, SocketGlobalVariables global) {
    this.sr = sr;
    this.globals = global;
  }

  @Override
  public void execute() {

    String nodeName = sr.getAsk()[1];
    String containerID = sr.getAsk()[2];
    globals.logContainerTime(containerID, System.currentTimeMillis());//更新最後更新時間

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
    }
  }

}
