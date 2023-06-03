package com.dslab.drs.rpc;

import static com.dslab.drs.monitor.MonitorConstant.CONTAINER_PREPARE;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import com.dslab.drs.container.ContainerTaskStatus;
import com.dslab.drs.container.Executor;
import com.dslab.drs.container.JRIExecutor;
import com.dslab.drs.drslog.DrsPhaseLogHelper;
import com.dslab.drs.utils.ProcessTimer;
import com.dslab.drs.yarn.application.ClientContext;
import com.dslab.drs.yarn.application.ContainerGlobalVariable;
import com.dslab.drs.yarn.application.DRSContainer;

public class TaskUmbilicalClient extends Thread {

  private static final Log LOG = LogFactory.getLog(TaskUmbilicalClient.class);

  boolean isRunning = true;
  private final String address;
  private final int port;
  private TaskUmbilicalProtocol proxy;
  private final String nodeName;
  private final String containerId;
  private final ClientContext context;
  private ContainerTaskStatus containerTaskStatus;

  private YarnConfiguration conf;

  public TaskUmbilicalClient(String address, int port, String nodeName, String containerId, YarnConfiguration conf,
          ClientContext context) {
    this.address = address;
    this.port = port;
    LOG.info("TaskUmbilicalClient port:"+port);
    this.nodeName = nodeName;
    this.containerId = containerId;
    this.conf = conf;
    this.context = context;
    this.containerTaskStatus = new ContainerTaskStatus();
    createProxy();
    
    
  }

  public void createProxy() {
    try {
      proxy = RPC.getProxy(TaskUmbilicalProtocol.class, TaskUmbilicalProtocol.versionID,
              new InetSocketAddress(address, port), conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  enum ClientStatus {
    Intial, Ask, WaitFinish, End
  }

  public void run() {
    ClientStatus clientStatus = ClientStatus.Intial;
    int count = 0;
    ResponseMessage responseMessage = null;

    String taskName = null;
    String taskUrl = null;
    long fileSize = 0;
    boolean taskStatus = false;

    com.dslab.drs.container.RTextConsole rtextconsole = null;
    Rengine engine = null;

    if (context.getConsoleto() != null) {
      rtextconsole = new com.dslab.drs.container.RTextConsole();
      engine = new Rengine(new String[]{"--no-save"}, false, rtextconsole);
    } else {
      engine = new Rengine(new String[]{"--no-save"}, false, null);
    }

    ProcessTimer timer = ProcessTimer.getTimer();
    long timeDiff = timer.getDifferenceMillis();
    DRSContainer.logStore.addPhaseLog(DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(),
            ContainerGlobalVariable.APPLICATION_NAME, containerId, null, CONTAINER_PREPARE, timeDiff));

    while (isRunning) {
      count++;

      LOG.debug(count + ",clientStatus:" + clientStatus);
      switch (clientStatus) {
        case Intial:
          RegisterMessage registerMessage = new RegisterMessage();
          registerMessage.setMessage("register");
          registerMessage.setNodeName(nodeName);
          registerMessage.setContainerId(containerId);
          responseMessage = proxy.register(registerMessage);
          break;

        case Ask:
          if (taskStatus) {
            DoneTaskMessage doneTaskMessage = new DoneTaskMessage();
            doneTaskMessage.setMessage("DoneTask");
            doneTaskMessage.setNodeName(nodeName);
            doneTaskMessage.setContainerId(containerId);
            doneTaskMessage.setFileName(taskName);
            doneTaskMessage.setFileUrl(taskUrl);
            responseMessage = proxy.done(doneTaskMessage);
          } else {
            FailedTaskMessage failedTaskMessage = new FailedTaskMessage();
            failedTaskMessage.setMessage("FailTask");
            failedTaskMessage.setNodeName(nodeName);
            failedTaskMessage.setContainerId(containerId);
            failedTaskMessage.setFileName(taskName);
            failedTaskMessage.setFileUrl(taskUrl);
            responseMessage = proxy.fail(failedTaskMessage);
          }
          break;

        case WaitFinish:
          AskStatusMessage askStatusMessage = new AskStatusMessage();
          askStatusMessage.setMessage("AskStatus");
          askStatusMessage.setNodeName(nodeName);
          askStatusMessage.setContainerId(containerId);
          responseMessage = proxy.askStatus(askStatusMessage);
          break;
      }

      if (responseMessage == null) {
        continue;
      } else {
        LOG.debug(count + ",Server:" + responseMessage.getMessage());
      }

      String message = responseMessage.getMessage();
      if (message.equals("NewTask")) {
        taskUrl = responseMessage.getFileUrl();
        taskName = responseMessage.getFileName();
        fileSize = responseMessage.getFileSize();
        ContainerGlobalVariable.FILENAME = taskName;
        ContainerGlobalVariable.FILE_URL = taskUrl;
        ContainerGlobalVariable.FILESIZE = fileSize;

        clientStatus = ClientStatus.Ask;
        taskStatus = false;
        Long timeBeforeR = System.currentTimeMillis();
        timer.settime();
        try {
          Executor JriClient;
          if (context.isConsoleEnable()) {
            JriClient = new JRIExecutor(engine, rtextconsole, conf, taskUrl, containerId, context);
          } else {
            JriClient = new JRIExecutor(engine, null, conf, taskUrl, containerId, context);
          }
          containerTaskStatus = JriClient.execute();
          taskStatus = containerTaskStatus.getResult();
        } catch (Exception ex) {
          LOG.error(ex.getMessage());
        } finally {
          ContainerGlobalVariable.FILENAME = null;
          ContainerGlobalVariable.FILE_URL = null;
          ContainerGlobalVariable.FILESIZE = 0;
        }
        Long timeAfterR = System.currentTimeMillis();
        LOG.info("task:" + taskName + ",R engine:" + (timeAfterR - timeBeforeR) + "(ms)");
      } else if (message.equals("Wait")) {
        clientStatus = ClientStatus.WaitFinish;
        try {
          Thread.sleep(3000); // Wait 等 3 秒再請求
        } catch (InterruptedException ex) {
          Logger.getLogger(TaskUmbilicalClient.class.getName()).log(Level.SEVERE, null, ex);
        }
      } else if (message.equals("Finish")) {
        clientStatus = clientStatusEnd();
      } else if (message.equals("ShutDone")) {
        clientStatus = clientStatusEnd();
      } else {
        LOG.debug("not finish or NewTask:" + message);
      }
    }

    REXP R_temp_directory_path = null;
    try {
      R_temp_directory_path = engine.eval("R_cleardir_result<-tempdir()");
    } catch (Exception e) {
      LOG.info("R getting tempdir() path encounter error");
      e.printStackTrace();
    }
    File RTmp = new File(R_temp_directory_path.asString());
    deleteDirectory(RTmp);
    deleteLocalFolder(conf, containerId, context);
    engine.end();
  }

  private ClientStatus clientStatusEnd() {
    isRunning = false;
    return ClientStatus.End;
  }

  private void deleteLocalFolder(YarnConfiguration conf, String container_id, ClientContext context) {
    LOG.info("Remove R local folder:/tmp/".concat(container_id));
    deleteDirectory(new File("/tmp/".concat(container_id)));
  }

  private boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
    }
    return (directory.delete());
  }

}
