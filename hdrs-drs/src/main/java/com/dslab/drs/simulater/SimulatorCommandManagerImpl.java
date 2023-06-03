package com.dslab.drs.simulater;

import com.dslab.drs.simulater.connection.hds.HdsRequester;
import com.dslab.drs.simulater.connection.resourceManager.RmRestfulApiRequestor;
import com.dslab.drs.simulater.connection.resourceManager.RmRestfulApiRequestorImpl;
import com.dslab.drs.simulater.resource.Resource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class SimulatorCommandManagerImpl implements SimulatorCommandManager {

  private static final Log LOG = LogFactory.getLog(SimulatorCommandManagerImpl.class);
  private final SimulatorConfManager confManager;
  private final SimulatorThreadManager threadManager;
  private final RmRestfulApiRequestor rmRestfulApiResquestor;
  private final SimulatorStatusInfos simulatorStatusInfos ;
  private final String rmHttpAddressPort;
  private int currentThreadId;
  

  public SimulatorCommandManagerImpl(SimulatorConfManager confManager, SimulatorThreadManager threadManager) throws IOException {
    this.confManager = confManager;
    this.threadManager = threadManager;
    this.rmRestfulApiResquestor = new RmRestfulApiRequestorImpl();
    this.rmHttpAddressPort = confManager.getRmHttpAddressPort();

    currentThreadId = 0;
    LOG.info("rmHttpAddressPort:" + rmHttpAddressPort);

    this.simulatorStatusInfos = new SimulatorStatusInfos(confManager,rmHttpAddressPort);
    
  }

  @Override
  public void init(SimulatorConfManager confManager) {
  }

  @Override
  public void start() {
    System.out.println("Starting...");
    int userCount = confManager.getUserCount();
    for (int userNumber = 0; userNumber < userCount; userNumber++) {
      addOne();
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping...");
    try {
//   列出正在執行的appId
      Set<String> runningAppIds = rmRestfulApiResquestor.getRunningAppId(rmHttpAddressPort);
//   關閉該些appId
      for (String appid : runningAppIds) {
        LOG.info("Try to kill " + appid + ".");
        killByAppId(appid);
      }
    } catch (IOException ex) {
      printErrorMessage(ex);
      LOG.error(ex);
    }
    threadManager.closeAll();
  }

  @Override
  public void addOne() {
    currentThreadId++;
    String threadName = "User_" + currentThreadId;
    System.out.println("Add application, " + threadName);

    try {
      DrsArguments drsArguments = confManager.prepareDrsFiles(threadName);
      LOG.info("Arguments:" + drsArguments.toString());

      DrsThread drsThread = threadManager.newDrsThread(threadName, drsArguments, confManager.getSimulaterConf(), confManager.getIsRunDrsAsync());
//      sleep(15000); //xml 和 inputfile，上傳後太快讀取會找不到檔案(DRS 的list 動作)，因此這部分折衷等個一秒。
      threadManager.runDrsThread(drsThread);
    } catch (IOException ex) {
      printErrorMessage(ex);
      LOG.error(ex);
    }
  }

  @Override
  public void killByAppId(String applicationId) {
    try {
      HdsRequester hdsRequestor = new HdsRequester();
      String killResult = hdsRequestor.killDrs(applicationId, confManager.getSimulaterConf());
      LOG.debug("killResult:" + killResult);
    } catch (IOException ex) {
      printErrorMessage(ex);
      LOG.error(ex);
    }
  }

  @Override
  public void printStatus() {
    try {
      System.out.println("Print status...");
      System.out.println("----------------");
      System.out.println("Running Threads:" + DrsThread.getRunningThread());

      simulatorStatusInfos.updataStatus();
      simulatorStatusInfos.printApplicationStatus();
      System.out.println("----------------");
      simulatorStatusInfos.printTotalCalulatedInfos();
      System.out.println("----------------");

    } catch (IOException | SQLException ex) {
      printErrorMessage(ex);
      LOG.error(ex);
    }

  }

  @Override
  public void clearLocalFiles() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void createFiles() {
    System.out.println("creating...");
    int fileCount = confManager.getFileCount();
    String fileCopyMethod = confManager.getFileCopyMethod();
    String copyFolder = confManager.getCopyFolder();
    String remoteFolder = confManager.getRemoteTempFolder();
    if (!remoteFolder.endsWith("/")) {
      remoteFolder = remoteFolder + "/";
    }
    String remoteResourceFolder = remoteFolder + Workload.RESOURCE_FOLDER;
    try {
      Resource copyer = new Resource();
      if ("CopyFromFolder".equals(fileCopyMethod)) {
        copyer.copyFromFolderToHds(fileCount, copyFolder, remoteResourceFolder, confManager.getFileExtension(), confManager.getSimulaterConf());
      } else if ("CopyFromOneFile".equals(fileCopyMethod)) {
        copyer.copyFromOneFileToHds(fileCount, copyFolder, remoteResourceFolder, confManager.getFileExtension(), confManager.getSimulaterConf());
      }
    } catch (IOException ex) {
      printErrorMessage(ex);
      LOG.error(ex);
    }
  }

  private void printErrorMessage(Exception ex) {
    ex.printStackTrace();
    System.out.println("failed.(" + ex.getMessage() + ")");
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (Exception ex) {
    }
  }
}
