package com.dslab.drs.socket;

import com.dslab.drs.tasks.FileInfo;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.yarn.application.ServerToClientMessage;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class AskStatus extends ContainerResponseState {

  private static final Log LOG = LogFactory.getLog(AskStatus.class);
  private final SocketRun sr;
  private final SocketGlobalVariables globals;
  private final DrsConfiguration conf;

  AskStatus(SocketRun sr, SocketGlobalVariables global, DrsConfiguration conf) {
    this.sr = sr;
    this.globals = global;
    this.conf = conf;
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
          if (conf.getBoolean(DrsConfiguration.DRS_RELEASE_IDLE_CONTAINER_ENABLE,
                  DrsConfiguration.DRS_RELEASE_IDLE_CONTAINER_ENABLE_DEFAULT)) {
            int memSize = globals.getContainerMng().getContainerResource(containerID).getMemory();
            if (SocketGlobalVariables.getAMGlobalVariable().isListThreadFinished()
                    && globals.getContainerMng().getContainerInterval(memSize).isContainerNowMoreThanOne()) {
              LOG.info("containerID:" + containerID + " idle -> release!");
              globals.getContainerMng().getContainerInterval(memSize).decreaseContainerExpectedNumNow();
              globals.addReleasedContainerID(containerID);
              ServerToClientMessage message = new ServerToClientMessage("Finish");
              outputMessage(message, sr.getSocket());
            } else {
              ServerToClientMessage message = new ServerToClientMessage("Wait");
              outputMessage(message, sr.getSocket());
            }
          } else {
            ServerToClientMessage message = new ServerToClientMessage("Wait");
            outputMessage(message, sr.getSocket());
          }
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
