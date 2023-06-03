package com.dslab.drs.yarn.application;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import com.dslab.drs.container.ContainerTaskStatus;
import com.dslab.drs.container.Executor;
import com.dslab.drs.container.JRIExecutor;
import com.dslab.drs.drslog.DrsPhaseLogHelper;
import com.dslab.drs.drslog.LogClientManager;
import com.dslab.drs.drslog.LogClientManagerImpl;
import static com.dslab.drs.monitor.MonitorConstant.CONTAINER_PREPARE;
import com.dslab.drs.monitor.ProcMemInfo;
import com.dslab.drs.utils.ProcessTimer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author CDCLAB
 */
public class SocketClient extends Thread {

  private static final Log LOG = LogFactory.getLog(SocketClient.class);

  boolean isRunning = true;
  private final String address;
  private int port = 5566;// 連線的port
  private Socket client_socket;
  private final String nodeName;
  private final String containerID;
  private final ClientContext context;
  private ContainerTaskStatus ContainerTaskInfo;
  private LogClientManager lcm;

  YarnConfiguration conf;

  public SocketClient(String address, int port, String nodeName, String containerID, YarnConfiguration conf, ClientContext context) {
    this.address = address;
    this.port = port;
    this.nodeName = nodeName;
    this.containerID = containerID;
    this.conf = conf;
    this.context = context;
    this.ContainerTaskInfo = new ContainerTaskStatus();
  }

  enum ClientStatus {
    Intial, Ask, WaitFinish, End
  }

  @Override
  public void run() {

    ClientStatus clientStatus = ClientStatus.Intial;
    int count = 0;
    ServerToClientMessage direction = null;

    String task_name = null;
    String task_url = null;
    long fileSize = 0;
    boolean task_status = false;

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
    DRSContainer.logStore.addPhaseLog(DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(), ContainerGlobalVariable.APPLICATION_NAME, containerID, null, CONTAINER_PREPARE, timeDiff));

    while (isRunning) {
      count++;
      try {
        client_socket = new Socket(address, port);//建立連線。(ip為伺服器端的ip，port為伺服器端開啟的port)
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }

      try {
        if (client_socket != null)//連線成功才繼續往下執行
        {
          client_socket.setSoTimeout(60000); //一分鐘內Server沒有回應就中斷 (5/15 caca test not true)
          ObjectOutputStream out = new ObjectOutputStream(client_socket.getOutputStream());
          LOG.debug(count + ",clientStatus:" + clientStatus);
          switch (clientStatus) {
            case Intial: {
              ContainerTaskInfo.setCommand("Register " + nodeName + " " + containerID);
              out.writeObject(ContainerTaskInfo);
              out.flush();

              try {
                ObjectInputStream inputStreamFromClient = new ObjectInputStream(client_socket.getInputStream());
                direction = (ServerToClientMessage) inputStreamFromClient.readObject();
              } catch (ClassNotFoundException ex) {
                LOG.info("ClassNotFoundException:" + ex.getMessage());
                continue;
              }

              break;
            }
            case Ask: {
              if (task_status) {
                ContainerTaskInfo.setCommand("DoneTask " + nodeName + " " + containerID + " " + task_name + " " + fileSize);
                ContainerTaskInfo.setProcMemInfo(ProcMemInfo.getInstance());
                out.writeObject(ContainerTaskInfo);
                System.gc();
              } else {
                ContainerTaskInfo.setCommand("FailTask " + nodeName + " " + containerID + " " + task_name + " " + fileSize);
                ContainerTaskInfo.setProcMemInfo(ProcMemInfo.getInstance());
                out.writeObject(ContainerTaskInfo);
                System.gc();
              }
              out.flush();

              try {
                ObjectInputStream inputStreamFromClient = new ObjectInputStream(client_socket.getInputStream());
                direction = (ServerToClientMessage) inputStreamFromClient.readObject();
              } catch (ClassNotFoundException ex) {
                LOG.info("ClassNotFoundException:" + ex.getMessage());
                continue;
              }
              break;
            }
            case WaitFinish: {
              ContainerTaskInfo.setCommand("AskStatus " + nodeName + " " + containerID);
              out.writeObject(ContainerTaskInfo);
              out.flush();

              try {
                ObjectInputStream inputStreamFromClient = new ObjectInputStream(client_socket.getInputStream());
                direction = (ServerToClientMessage) inputStreamFromClient.readObject();
              } catch (ClassNotFoundException ex) {
                LOG.info("ClassNotFoundException:" + ex.getMessage());
                continue;
              }
              break;
            }
          }

          client_socket.close();
        }
      } catch (IOException e) {
        //如果readline 太久沒接收到回應會有Read timed out
        LOG.error(e.getMessage());
      }

      if (direction == null) {
        continue;
      } else {
        LOG.debug(count + ",Server:" + direction.getStatus());
      }

      String currentStatus = direction.getStatus();

//            String[] direction_splite = direction.split(" ");
      if (currentStatus.equals("NewTask")) {
        lcm.startMonitor();
        task_url = direction.geUrl();
        task_name = direction.getFileName();
        fileSize = direction.getFileSize();
        ContainerGlobalVariable.FILENAME = task_name;
        ContainerGlobalVariable.FILE_URL = task_url;
        ContainerGlobalVariable.FILESIZE = fileSize;

        clientStatus = ClientStatus.Ask;
        task_status = false;

        Long timeBeforeR = System.currentTimeMillis();
        timer.settime();
        try {
          Executor JriClient;
          if (context.isConsoleEnable()) {
            JriClient = new JRIExecutor(engine, rtextconsole, conf, task_url, containerID, context);
          } else {
            JriClient = new JRIExecutor(engine, null, conf, task_url, containerID, context);
          }
          ContainerTaskInfo = JriClient.execute();

          task_status = ContainerTaskInfo.getResult();
        } catch (Exception ex) {
          LOG.error(ex.getMessage());
        } finally {
          lcm.stopMonitor();
          ContainerGlobalVariable.FILENAME = null;
          ContainerGlobalVariable.FILE_URL = null;
          ContainerGlobalVariable.FILESIZE = 0;
        }
        Long timeAfterR = System.currentTimeMillis();
        LOG.info("task:" + task_name
                + ",R engine:" + (timeAfterR - timeBeforeR) + "(ms)");

      } else if (currentStatus.equals("Wait")) {
        clientStatus = ClientStatus.WaitFinish;
        try {
          TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException ex) {
          Logger.getLogger(SocketClient.class.getName()).log(Level.SEVERE, null, ex);
        }
      } else if (currentStatus.equals("Finish")) {
        LOG.info("Receive Finish from server.");
        clientStatus = clientStatusEnd();
      } else {
        LOG.debug("not finish or NewTask:" + currentStatus);
      }
    }   //end while

    //clear RTmp direcotry in /tmp (EX:/tmp/RtmpoRlz31) 
    REXP R_temp_directory_path = null;
    try {
      R_temp_directory_path = engine.eval("R_cleardir_result<-tempdir()");
    } catch (Exception e) {
      LOG.info("R getting tempdir() path encounter error");
      e.printStackTrace();
    }
    File RTmp = new File(R_temp_directory_path.asString());
    deleteDirectory(RTmp);
    deleteLocalFolder(conf, containerID, context);
    engine.end();
  }

  private ClientStatus clientStatusEnd() {
    isRunning = false;
    return ClientStatus.End;
  }

  public void stopThread() {
    this.isRunning = false;
  }

  public void add(LogClientManager lcm) {
    this.lcm = lcm;
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
