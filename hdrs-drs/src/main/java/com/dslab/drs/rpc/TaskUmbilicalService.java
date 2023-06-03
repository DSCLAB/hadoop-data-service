//package com.dslab.drs.rpc;
//
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.net.ServerSocket;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.logging.Level;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.HadoopIllegalArgumentException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.ipc.ProtocolSignature;
//import org.apache.hadoop.ipc.RPC;
//import org.apache.hadoop.ipc.Server;
//import org.apache.hadoop.net.NetUtils;
//
//import com.dslab.drs.api.ServiceStatus;
//import com.dslab.drs.container.ContainerTaskStatus;
//import com.dslab.drs.drslog.LogPacket;
//import com.dslab.drs.drslog.LogServerManager;
//import com.dslab.drs.drslog.LogServerManagerImpl;
//import com.dslab.drs.socket.ResponseType;
//import com.dslab.drs.socket.SocketRun;
//import com.dslab.drs.socket.StateFactory;
//import com.dslab.drs.tasks.FileInfo;
//import com.dslab.drs.tasks.Scheduler;
//import com.dslab.drs.utils.ProcessTimer;
//import com.dslab.drs.yarn.application.ApplicationMaster;
////import com.dslab.drs.yarn.application.SocketServer;
//
//public class TaskUmbilicalService extends Thread implements TaskUmbilicalProtocol {
//
//  private static final Log LOG = LogFactory.getLog(TaskUmbilicalService.class);
//
//  private Configuration conf;
//
//  private boolean isRunning;
//  private boolean isKilled;
//  private ApplicationMaster am;
//  private Scheduler scheduler_v2;
//
//  private int runningContainerCount;
//
//  private int registerContainerCount;
//  private int containerFinishCount;
//  private int fileSuccessCount;
//  private boolean DRSClientRunning = false;
//  private boolean watchRequestFlag = false;
//
//  private ServiceStatus serviceStatus;
//
//  private HashMap<String, List<String>> containerIdTaskMap = new HashMap<>();
//  private HashMap<String, Long> containerLastResponseTime = new HashMap<>();
//
//  private Server server;
//  private ServerSocket drsClientServer;
//
//  private int serverPort = 0;
//  private int drsClientServerPort = 0;
//
//  private FileSystem fs;
//  private Path portPath;
//  private Path statusPath;
//
//  private int NumberOfContainers = 0;
//  private String code;
//  private String copyto;
//
//  enum ServerStats {
//    NewTask, FailTask, DoneTask
//  }
//
//  public TaskUmbilicalService(Scheduler scheduler_v2, FileSystem fs, String sharePath, ApplicationMaster am,
//          long startTime) {
//    this.am = am;
//    this.fs = fs;
//    this.scheduler_v2 = scheduler_v2;
//    this.runningContainerCount = scheduler_v2.getHostList().size();
//    this.portPath = new Path(sharePath + "/port");
//    this.statusPath = new Path(sharePath + "/serviceStatus");
//
//    Map<String, String> env = System.getenv();
//    String data = env.get("DATA");
//
//    serviceStatus = new ServiceStatus(scheduler_v2.getInputFileInfos(), NetUtils.getHostname());
//    serviceStatus.setHdsFileInfoCollectTimeMs(scheduler_v2.getHdsFileInfoCollectTimeMs());
//    serviceStatus.setClientContext(env.get("R_CODE"), env.get("DATA"), env.get("CONFIG"), env.get("COPYTO"));
//    serviceStatus.setStartTime(startTime);
//    serviceStatus.setStatus("RUNNING");
//    try {
//      drsClientServer = new ServerSocket(drsClientServerPort);
//    } catch (IOException e) {
//      LOG.error("Socket Start Fail");
//      LOG.error("IOException :" + e.toString());
//    }
//
//    try {
//      if (fs.exists(portPath)) {
//        fs.delete(portPath, true);
//        LOG.info("Delete port log:" + portPath.toString());
//      }
//      // sava port value to hdsfs
//      FSDataOutputStream fsStream = fs.create(portPath);
//      drsClientServerPort = drsClientServer.getLocalPort();
//      LOG.info("Socket server port:" + drsClientServerPort);
//      fsStream.writeUTF(String.valueOf(drsClientServerPort));
//      fsStream.close();
//
//    } catch (Exception e) {
//      LOG.info("Log socket port error.");
//      LOG.error(e.getMessage());
//    }
//
//    serviceInit();
//  }
//
//  public void serviceInit() {
//    isRunning = true;
//    isKilled = false;
//    registerContainerCount = 0;
//    containerFinishCount = 0;
//    fileSuccessCount = 0;
//    conf = new Configuration();
//
//    startRpcServer();
//  }
//
//  public void startRpcServer() {
//    System.out.println("Updated!!!");
//    try {
//      server = new RPC.Builder(conf).setProtocol(TaskUmbilicalProtocol.class).setInstance(this)
//              .setBindAddress("0.0.0.0").setPort(serverPort).setNumHandlers(1).build();
//      System.out.println("get rpc Port:" + server.getPort());
//    } catch (HadoopIllegalArgumentException e) {
//      e.printStackTrace();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    server.start();
//  }
//
//  @Override
//  public ResponseMessage register(RegisterMessage msg) {
//    System.out.println("call register function()");
//    String nodeName = msg.getNodeName();
//    String containerId = msg.getContainerId();
//
//    ResponseMessage response = new ResponseMessage();
//    registerContainerCount++;
//    containerLastResponseTime.put(containerId, System.currentTimeMillis());
//
//    FileInfo fileInfo = null;
//    if (!scheduler_v2.hasFreeTasks()) {
//      response.setMessage("Wait");
//    } else {
//      fileInfo = scheduler_v2.getNewTask(nodeName, containerId, runningContainerCount);
//      scheduler_v2.getNodesStatus().get(containerId).setWorkingTask(fileInfo.getName());
//      containerIdTaskMap.put(containerId, Collections.singletonList(fileInfo.getName()));
//      String fileUrl = fileInfo.getUrl();
//      String fileName = fileInfo.getName();
//      long fileSize = fileInfo.getSize();
//      response.setMessage("NewTask");
//      response.setFileUrl(fileUrl);
//      response.setFileName(fileName);
//      response.setFileSize(fileSize);
//    }
//    return response;
//  }
//
//  @Override
//  public ResponseMessage done(DoneTaskMessage msg) {
//    System.out.println("call done function()");
//    String fileUrl = msg.getFileUrl();
//    String fileName = msg.getFileName();
//    String nodeName = msg.getNodeName();
//    String containerId = msg.getContainerId();
//
//    ResponseMessage response = new ResponseMessage();
//    fileSuccessCount++;
//    containerLastResponseTime.put(containerId, System.currentTimeMillis());
//    scheduler_v2.markSuccess(fileName, nodeName, containerId);
//    containerIdTaskMap.remove(containerId);
//
//    FileInfo fileInfo = null;
//    if (!scheduler_v2.hasFreeTasks()) {
//      am.expectedContainers--;
//      response.setMessage("Finish");
//    } else {
//      fileInfo = scheduler_v2.getNewTask(nodeName, containerId, runningContainerCount);
//      scheduler_v2.getNodesStatus().get(containerId).setWorkingTask(fileInfo.getName());
//      containerIdTaskMap.put(containerId, Collections.singletonList(fileInfo.getName()));
//      String newFileUrl = fileInfo.getUrl();
//      String newFileName = fileInfo.getName();
//      long newFileSize = fileInfo.getSize();
//      response.setMessage("NewTask");
//      response.setFileUrl(newFileUrl);
//      response.setFileName(newFileName);
//      response.setFileSize(newFileSize);
//    }
//    return response;
//  }
//
//  @Override
//  public ResponseMessage fail(FailedTaskMessage msg) {
//    System.out.println("call fail function()");
//    String fileUrl = msg.getFileUrl();
//    String fileName = msg.getFileName();
//    String nodeName = msg.getNodeName();
//    String containerId = msg.getContainerId();
//    String errorMessage = msg.getErrorMessage();
//
//    scheduler_v2.markFail(fileName, nodeName, containerId, errorMessage);
//    containerLastResponseTime.put(containerId, System.currentTimeMillis());
//
//    ResponseMessage response = new ResponseMessage();
//    response.setMessage("Wait");
//
//    return response;
//  }
//
//  @Override
//  public ResponseMessage askStatus(AskStatusMessage msg) {
//    System.out.println("call AskStatus function()");
//    String nodeName = msg.getNodeName();
//    String containerId = msg.getContainerId();
//
//    ResponseMessage response = new ResponseMessage();
//
//    containerLastResponseTime.put(containerId, System.currentTimeMillis());
//
//    FileInfo fileInfo = null;
//    if (scheduler_v2.hasFreeTasks()) {
//      fileInfo = scheduler_v2.getNewTask(nodeName, containerId, runningContainerCount);
//      scheduler_v2.getNodesStatus().get(containerId).setWorkingTask(fileInfo.getName());
//      containerIdTaskMap.put(containerId, Collections.singletonList(fileInfo.getName()));
//
//      String fileUrl = fileInfo.getUrl();
//      String fileName = fileInfo.getName();
//      long fileSize = fileInfo.getSize();
//      response.setMessage("NewTask");
//      response.setFileUrl(fileUrl);
//      response.setFileName(fileName);
//      response.setFileSize(fileSize);
//    }
//
//    return response;
//  }
//
//  @Override
//  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
//    return TaskUmbilicalProtocol.versionID;
//  }
//
//  @Override
//  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
//          throws IOException {
//    return new ProtocolSignature(TaskUmbilicalProtocol.versionID, null);
//  }
//
//  public void closeServerSucket() {
//    isRunning = false;
//    try {
//      this.drsClientServer.close();
//    } catch (Exception ex) {
//      java.util.logging.Logger.getLogger(TaskUmbilicalService.class.getName()).log(Level.SEVERE, null, ex);
//    }
//  }
//
//  @Override
//  public void run() {
//    ProcessTimer timer = ProcessTimer.getTimer();
//    double diff = timer.getDifferenceMillis();
//    int coount = 0;
//    try {
//      int count = 0;
//      SocketRun sr = new SocketRun();
//      LOG.debug("Waitting for Connection......");
//
//      LogServerManager logServerManager = LogServerManagerImpl.getLogServerManagerImpl(null);
//      while (isRunning) {
//        // boolean watchRequestFlag = false;
//        try {
//          count++;
//
//          try {
//            sr.setSocket(drsClientServer.accept()); // 等待伺服器端的連線，若未連線則程式一直停在這裡
//          } catch (Exception ex) {
//            LOG.info("server.accept() exception," + ex.toString());
//            continue;
//          }
//          // System.out.println("Get Connect " + count);
//          // schedule_v2.listFileInfos(schedule_v2.fileInfos);
//          // 送出端的編寫必須和接收端的接收Class相同
//          // 使用Socket的getInputStream()和getOutputStream()進行接收和發送資料
//          // 想要寫入字串可以用 PrintStream；想要把各種基本資料型態，如 int, double...等的 "值"
//          // 輸出，可以用 DataOutputStream；想要把整個物件 Serialize，則可以用
//          // ObjectOutputStream。
//          // PrintStream writer;//在此我使用PrintStream將字串進行編寫和送出
//
//          ObjectInputStream inputStreamFromClient = new ObjectInputStream(sr.getSocket().getInputStream());
//          // ContainerTaskStatus containerTaskStatus;
//
//          try {
//            Object clientObject = inputStreamFromClient.readObject();
//            try {
//              sr.setContainerTaskStatus((ContainerTaskStatus) clientObject);
//            } catch (Exception ex) {
//              LogPacket logPacket = (LogPacket) clientObject;
//              // LOG.info("###Size:{},{},{}" +
//              // logPacket.getToalCellSize(),
//              // logPacket.getPhaseList().size(),
//              // logPacket.getResourceList().size());
//              logServerManager.addLogPacket(logPacket);
//              sr.getSocket().close();
//              continue;
//            }
//            // containerTaskStatus = (ContainerTaskStatus)
//            // inputStreamFromClient.readObject();
//          } catch (ClassNotFoundException ex) {
//            LOG.info("ClassNotFoundException:" + ex.getMessage());
//            continue;
//          }
//
//          String clientAsk = sr.getContainerTaskStatus().getCommand();
//          LOG.debug("Client : " + clientAsk);// 讀取一行字串資料
//          sr.setAsk(clientAsk.split(" "));
//
//          ResponseType type = askTransformToType(sr.getAsk()[0]);
//          if (scheduler_v2.isComplete()
//                  && !(type == ResponseType.APPCLI_ASK_PROGRESS || type == ResponseType.DRSCLI_KILL
//                  || type == ResponseType.APPCLI_REGISTER || type == ResponseType.WATCH_REQ)) {
//            type = ResponseType.SCHEDULE_SUCCESS;
//          }
//          StateFactory sf = new StateFactory();
//          sf.getResponseType(type, sr, this).execute();
//
//        } catch (IOException e) {
//          e.printStackTrace();
//          LOG.error(e.toString()); // 出現例外時，捕捉並顯示例外訊息(連線成功不會出現例外)
//          LOG.error(e.getMessage());
//        } finally {
//          if (sr.getSocket() != null) {
//            sr.getSocket().close();
//          }
//        }
//
//        if (scheduler_v2.isComplete() && !sr.getDRSClientRunning() && !sr.getWatchRequestFlag()) { // 沒有DRSClient再跑
//          // 跳出迴圈
//          // isRunning = false;
//          setIsRunning(false);
//
//          server.stop();
//
//          LOG.info("finish_count:" + sr.getContainerFinishCount() + ",register_count:"
//                  + sr.getRegisterCount());
//          LOG.info("DRSClientRunning:" + sr.getDRSClientRunning());
//          LOG.info("TaskUmbilicalService.java close server .");
//        }
//      } // end while
//      LOG.debug("server.close()");
//      drsClientServer.close();// 跳出while loop才關掉
//
//    } catch (IOException ex) {
//      java.util.logging.Logger.getLogger(TaskUmbilicalService.class.getName()).log(Level.SEVERE, null, ex);
//    }
//
//    timer = ProcessTimer.getTimer();
//    double dif = timer.getDifferenceMillis();
//
//    LOG.debug(getServiceStatus().toJSON());
//    LOG.debug("Time different between response register to TaskUmbilicalService end :" + dif);
//  }
//
//  private ResponseType askTransformToType(String type) throws IOException {
//    switch (type) {
//      case "AppClientAskProgress":
//        return ResponseType.APPCLI_ASK_PROGRESS;
//      case "DrsClientKill":
//        return ResponseType.DRSCLI_KILL;
//      case "AppClientRegister":
//        return ResponseType.APPCLI_REGISTER;
//      case "WatchRequest":
//        return ResponseType.WATCH_REQ;
//      case "Progress":
//        return ResponseType.PROGRESS;
//      case "Register":
//        return ResponseType.REGISTER;
//      case "NewTask":
//        return ResponseType.NEW_TASK;
//      case "FailTask":
//        return ResponseType.FAILED_TASK;
//      case "DoneTask":
//        return ResponseType.DONE_TASK;
//      case "AskStatus":
//        return ResponseType.ASK_STATUS;
//      default:
//        return ResponseType.WRONG_REQ;
//    }
//  }
//
//  public boolean isServerRunning() {
//    return this.isRunning;
//  }
//
//  public boolean isServerKilled() {
//    return this.isKilled;
//  }
//
//  public Scheduler getScheduler() {
//    return this.scheduler_v2;
//  }
//
//  public int getRunningConteinerCount() {
//    return this.runningContainerCount;
//  }
//
//  public ServiceStatus getServiceStatus() {
//    return this.serviceStatus;
//  }
//
//  public ApplicationMaster getAM() {
//    return this.am;
//  }
//
//  public HashMap<String, List<String>> getContaineridTaskMap() {
//    return this.containerIdTaskMap;
//  }
//
//  public void containerRunningTaskMark(String containerId, String taskName) {
//    containerIdTaskMap.put(containerId, Collections.singletonList(taskName));
//  }
//
//  public HashMap<String, Long> getContainerLastResponseTime() {
//    return this.containerLastResponseTime;
//  }
//
//  public void logContainerTime(String containerID) {
//    containerLastResponseTime.put(containerID, System.currentTimeMillis());
//  }
//
//  public void setIsRunning(boolean b) {
//    this.isRunning = b;
//  }
//
//  public void setIsKilled(boolean b) {
//    this.isKilled = b;
//  }
//
//  public void setAM(ApplicationMaster am) {
//    this.am = am;
//  }
//
//  public void decreaseAMExpectedContainer() {
//    am.expectedContainers = am.expectedContainers - 1;
//  }
//
//  public void setScheduler(Scheduler scheduler) {
//    this.scheduler_v2 = scheduler;
//  }
//
//  public void increaseRunningConteinerCount() {
//    this.runningContainerCount++;
//  }
//
//  public void decreaseRunningConteinerCount() {
//    this.runningContainerCount--;
//  }
//
//  public void setRunningConteinerCount(int runningConteinerCount) {
//    this.runningContainerCount = runningConteinerCount;
//  }
//
//  public void setServiceStatus(ServiceStatus serviceStatus) {
//    this.serviceStatus = serviceStatus;
//  }
//
//  public void setContaineridTaskMapd(HashMap<String, List<String>> containeridTaskMap) {
//    this.containerIdTaskMap = containeridTaskMap;
//  }
//
//  public boolean isContainerHasTask(String containerId, String nodeName) {
//    if (containerIdTaskMap.get(containerId) != null) {
//      List<String> tempList = containerIdTaskMap.get(containerId);
//      if (tempList.size() > 0) {
//        return true;
//      }
//    }
//    return false;
//  }
//
//  public void clearContainerRunningTask(String containerId, String nodeName) {
//
//    if (containerIdTaskMap.get(containerId) != null) {
//      List<String> temp = containerIdTaskMap.get(containerId);
//      for (String taskName : temp) {
//        scheduler_v2.markFail(taskName, nodeName, containerId, "Container Fail");
//      }
//    }
//  }
//
//  public String getContainerRunningTasks(String containerId) {
//    StringBuilder taskBuilder = new StringBuilder("");
//    if (containerIdTaskMap.get(containerId) != null) {
//      List<String> temp = containerIdTaskMap.get(containerId);
//      temp.stream().forEach((taskName) -> {
//        taskBuilder.append(taskName).append(" ");
//      });
//    }
//    return taskBuilder.toString();
//  }
//  // soft stop
//
//  public void stopThread() {
//    // this.isRunning = false;
//    setIsRunning(false);
//  }
//
//  public int getPort() {
//    return this.server.getPort();
//  }
//
//  public void setNumberOfContainers(int numOfContainers) {
//    this.NumberOfContainers = numOfContainers;
//  }
//
//}
