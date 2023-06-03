package com.dslab.drs.yarn.application;

import com.dslab.drs.tasks.Scheduler;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.util.Map;
import java.util.logging.Level;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.dslab.drs.container.ContainerTaskStatus;
import com.dslab.drs.drslog.LogPacket;
import com.dslab.drs.drslog.LogServerManager;
import com.dslab.drs.drslog.LogServerManagerImpl;
import com.dslab.drs.socket.ResponseType;
import com.dslab.drs.socket.SocketGlobalVariables;
import com.dslab.drs.socket.SocketRun;
import com.dslab.drs.socket.StateFactory;
import com.dslab.drs.tasks.FileInfoStorage;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.utils.ProcessTimer;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author CDCLAB
 */
public class SocketServer extends Thread {

  private static final Log LOG = LogFactory.getLog(SocketServer.class);
  SocketGlobalVariables globals = SocketGlobalVariables.getAMGlobalVariable();
  private String code;
  private String copyto;

  private boolean OutServer = false;
  private ServerSocket server;
  private FileSystem fs;

  private int serverPort = 0;// 自由分配port
  private Path portPath;
  private Path statusPath;
  private final DrsConfiguration conf;

  enum ServerStats {
    NewTask, FailTask, DoneTask
  }

  public SocketServer(Scheduler schedule_v2, FileSystem fs, String sharePath, long startTime, Configuration conf) {
    this.fs = fs;
    globals.setScheduler(schedule_v2);
    this.portPath = new Path(sharePath + "/port");
    this.statusPath = new Path(sharePath + "/serviceStatus");
    this.conf = new DrsConfiguration(conf);

    Map<String, String> env = System.getenv();
    String data = env.get("DATA");
    globals.getServiceStatus().setHdsFileInfoCollectTimeMs(schedule_v2.getHdsFileInfoCollectTimeMs());
    globals.getServiceStatus().setClientContext(env.get("R_CODE"), env.get("DATA"), env.get("CONFIG"), env.get("COPYTO"));
    globals.getServiceStatus().setStartTime(startTime);
    globals.getServiceStatus().setStatus("RUNNING");
    try {
      server = new ServerSocket(serverPort);
    } catch (IOException e) {
      LOG.error("Socket Start Fail");
      LOG.error("IOException :" + e.toString());
    }

    try {
      if (fs.exists(portPath)) {
        fs.delete(portPath, true);
        LOG.info("Delete port log:" + portPath.toString());
      }
      //sava port value to hdsfs
      FSDataOutputStream fsStream = fs.create(portPath);
      serverPort = server.getLocalPort();
      LOG.info("Socket server port:" + serverPort);
      fsStream.writeUTF(String.valueOf(serverPort));
      fsStream.close();

    } catch (Exception e) {
      LOG.info("Log socket port error.");
      LOG.error(e.getMessage());
    }

  }

  public void closeServerSucket() {
    globals.setIsRunning(false);
    try {
      this.server.close();
    } catch (Exception ex) {
      java.util.logging.Logger.getLogger(SocketServer.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public boolean isServerRunning() {
    return this.globals.isRunning();
  }

  public boolean isServerKilled() {
    return this.globals.isKilled();
  }

  @Override
  public void run() {
    ProcessTimer timer = ProcessTimer.getTimer();
    double diff = timer.getDifferenceMillis();
    try {
      int count = 0;
      SocketRun socketRun = new SocketRun();
      LOG.debug("Waitting for Connection......");

      LogServerManager logServerManager = LogServerManagerImpl.getLogServerManagerImpl(null);
      while (globals.isRunning()) {
//        boolean watchRequestFlag = false;
        try {
          count++;
          try {
            socketRun.setSocket(server.accept()); //等待伺服器端的連線，若未連線則程式一直停在這裡
          } catch (Exception ex) {
            LOG.info("server.accept() exception," + ex.toString());
            continue;
          }
//          System.out.println("Get  Connect " + count);
// schedule_v2.listFileInfos(schedule_v2.fileInfos);
          //送出端的編寫必須和接收端的接收Class相同
          //使用Socket的getInputStream()和getOutputStream()進行接收和發送資料
          //想要寫入字串可以用 PrintStream；想要把各種基本資料型態，如 int, double...等的 "值" 輸出，可以用 DataOutputStream；想要把整個物件 Serialize，則可以用 ObjectOutputStream。
          //                   PrintStream writer;//在此我使用PrintStream將字串進行編寫和送出

          ObjectInputStream inputStreamFromClient = new ObjectInputStream(socketRun.getSocket().getInputStream());
//          ContainerTaskStatus containerTaskStatus;

          try {
            Object clientObject = inputStreamFromClient.readObject();
            try {
              socketRun.setContainerTaskStatus((ContainerTaskStatus) clientObject);
            } catch (Exception ex) {
              LogPacket logPacket = (LogPacket) clientObject;
//              LOG.info("###Size:{},{},{}" + logPacket.getToalCellSize(), logPacket.getPhaseList().size(), logPacket.getResourceList().size());
              logServerManager.addLogPacket(logPacket);
              socketRun.getSocket().close();
              continue;
            }
//            containerTaskStatus = (ContainerTaskStatus) inputStreamFromClient.readObject();
//            containerTaskStatus = (ContainerTaskStatus) inputStreamFromClient.readObject();
          } catch (ClassNotFoundException ex) {
            LOG.info("ClassNotFoundException:" + ex.getMessage());
            continue;
          }

          String clientAsk = socketRun.getContainerTaskStatus().getCommand();
          LOG.debug("Client : " + clientAsk);//讀取一行字串資料
          socketRun.setAsk(clientAsk.split(" "));

          ResponseType type = toResponseType(socketRun.getAsk()[0]);
          if (globals.getScheduler().isComplete()
                  && !(type == ResponseType.APPCLI_ASK_PROGRESS
                  || type == ResponseType.DRSCLI_KILL
                  || type == ResponseType.APPCLI_REGISTER
                  || type == ResponseType.WATCH_REQ)) {
            type = ResponseType.SCHEDULE_SUCCESS;
          }
          StateFactory sf = new StateFactory();
          sf.getResponseType(type, socketRun, globals, conf).execute();

        } catch (IOException e) {
          e.printStackTrace();
          LOG.error(e.toString());           //出現例外時，捕捉並顯示例外訊息(連線成功不會出現例外)
          LOG.error(e.getMessage());
        } finally {
          if (socketRun.getSocket() != null) {
            socketRun.getSocket().close();
          }
        }

        if (!FileInfoStorage.getFileInfoStorage().listDone()
                && socketRun.getContainerFinishCount() == socketRun.getRegisterCount()
                && !socketRun.getDRSClientRunning() && !socketRun.getWatchRequestFlag()) {  //沒有DRSClient再跑  跳出迴圈
          globals.setIsRunning(false);
          LOG.info("finish_count:" + socketRun.getContainerFinishCount() + ",register_count:" + socketRun.getRegisterCount());
          LOG.info("DRSClientRunning:" + socketRun.getDRSClientRunning());
          LOG.info("SocketServer.java close server .");
        }
      }   //end while
      LOG.debug("server.close()");
      server.close();//跳出while loop才關掉

    } catch (IOException ex) {
      java.util.logging.Logger.getLogger(SocketServer.class.getName()).log(Level.SEVERE, null, ex);
    }

    timer = ProcessTimer.getTimer();
    double dif = timer.getDifferenceMillis();

    LOG.debug(globals.getServiceStatus().toJSON());
    LOG.debug("Time different between response register to SocketServer end :" + dif);
  }

  private ResponseType toResponseType(String type) throws IOException {
    switch (type) {
      case "AppClientAskProgress":
        return ResponseType.APPCLI_ASK_PROGRESS;
      case "DrsClientKill":
        return ResponseType.DRSCLI_KILL;
      case "AppClientRegister":
        return ResponseType.APPCLI_REGISTER;
      case "WatchRequest":
        return ResponseType.WATCH_REQ;
      case "Progress":
        return ResponseType.PROGRESS;
      case "Register":
        return ResponseType.REGISTER;
      case "NewTask":
        return ResponseType.NEW_TASK;
      case "FailTask":
        return ResponseType.FAILED_TASK;
      case "DoneTask":
        return ResponseType.DONE_TASK;
      case "AskStatus":
        return ResponseType.ASK_STATUS;
      default:
        return ResponseType.WRONG_REQ;
    }
  }

  public boolean isContainerHasTask(String containerId, String nodeName) {
    if (globals.getContaineridTaskMap().get(containerId) != null) {
      if (globals.getContaineridTaskMap().get(containerId).size() > 0) {
        return true;
      }
    }
    return false;
  }

  public void clearContainerRunningTask(String containerId, String nodeName, boolean isPreempted) {

    if (globals.getContaineridTaskMap().get(containerId) != null) {
      List<String> temp = globals.getContaineridTaskMap().get(containerId);
      for (String taskName : temp) {
        if (isPreempted) {
          globals.getScheduler().markFail(taskName, nodeName, containerId, "Preempted", isPreempted);
        } else {
          globals.getScheduler().markFail(taskName, nodeName, containerId, "Container Fail", isPreempted);
        }
      }
    }
  }

  public String getContainerRunningTasks(String containerId) {
    StringBuilder taskBuilder = new StringBuilder("");
    if (globals.getContaineridTaskMap().get(containerId) != null) {
      List<String> temp = globals.getContaineridTaskMap().get(containerId);
      temp.stream().forEach((taskName) -> {
        taskBuilder.append(taskName).append(" ");
      });
    }
    return taskBuilder.toString();
  }
  //soft stop

  public void stopThread() {
//    this.isRunning = false;
    globals.setIsRunning(false);
  }

  public int getPort() {
    return this.serverPort;
  }

}
