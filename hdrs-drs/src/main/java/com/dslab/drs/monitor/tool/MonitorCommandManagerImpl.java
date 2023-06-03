package com.dslab.drs.monitor.tool;

import com.dslab.drs.restful.api.response.watch.WatchResult;
import com.dslab.drs.simulater.connection.jdbc.ApplicationResourceInfos;
import com.dslab.drs.simulater.connection.jdbc.DbCollector;
import com.dslab.drs.simulater.connection.resourceManager.RmRestfulApiRequestor;
import com.dslab.drs.simulater.connection.resourceManager.RmRestfulApiRequestorImpl;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author kh87313
 */
public class MonitorCommandManagerImpl implements MonitorCommandManager {

  private static final Log LOG = LogFactory.getLog(MonitorCommandManagerImpl.class);

  private final MonitorConfManager monitorConfManager;
  private final RmRestfulApiRequestor rmRestfulApiResquestor;
  private final String rmHttpAddressPort;

  private final DbCollector dbCollector;

  public MonitorCommandManagerImpl(MonitorConfManager monitorConfManager) throws IOException {
    this.monitorConfManager = monitorConfManager;
    this.rmRestfulApiResquestor = new RmRestfulApiRequestorImpl();
    this.rmHttpAddressPort = monitorConfManager.getRmHttpAddressPort();
    LOG.info("rmHttpAddressPort:" + rmHttpAddressPort);
    this.dbCollector = DbCollector.getDbCollector(monitorConfManager.getDbConf());

  }

  @Override
  public void printRunningJobs() {
    System.out.println("==================");
    System.out.println("Running Jobs:");

    String nl = System.getProperty("line.separator");
    try {
      Set<String> runningJobs = rmRestfulApiResquestor.getRunningAppId(rmHttpAddressPort);
      for (String jonId : runningJobs) {
        System.out.printf("%32s" + nl, jonId);
      }
      System.out.println("Total " + runningJobs.size() + " jobs.");
    } catch (IOException ex) {
      printErrorMessage(ex);
      LOG.error(ex);
    }
  }

  @Override
  public void printJobStatus(String appId) {
    if (dbCollector.isApplicationIdExistOnTable(appId)) {
      printDbResult(appId);
//      System.out.println("-----");
      printDbPhaseResult(appId);
    } else {
      System.out.println("Can't get " + appId + " Info from DbLog.(No data)");
    }
    System.out.println("-----");
    printWatchResult(appId, monitorConfManager.getMonitorConf());
  }

  @Override
  public void quit() {
  }

  private void printDbResult(String appId) {

    Optional<ApplicationResourceInfos> applicationResourceInfos = dbCollector.getApplicationResourceInfos(appId);
    Optional<Long> requestMemory = dbCollector.getRequestMemory(appId);

    String appIdTitle = "ApplicationId";
    String cpuTitle = "Cpu(Avg/Max)";
    String r_MemTitle = "R Mem(Avg/Max)";
    String jvm_MemTitle = "Jvm Mem(Avg/Max)";
    String proc_MemTitle = "Proc Mem(Avg/Max)";
    String prc_MemRateTitle = "Avg Mem Used Rate";

    System.out.printf("%-32s%-20s%-20s%-20s%-20s%-20s" + System.getProperty("line.separator"), appIdTitle, cpuTitle, r_MemTitle, jvm_MemTitle, proc_MemTitle, prc_MemRateTitle);

    long avgCpu = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getAvgCpu() : 0;
    long maxCpu = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getMaxCpu() : 0;;

    long avgRMem = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getAvgRMem() : 0;
    long maxRMem = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getMaxRMem() : 0;

    long avgJvmMem = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getAvgJvmMem() : 0;
    long maxJvmMem = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getMaxJvmMem() : 0;

    long avgProcMem = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getAvgProcMem() : 0;
    long maxProcMem = applicationResourceInfos.isPresent() ? applicationResourceInfos.get().getMaxProcMem() : 0;

    long mb = 1024 * 1024;
    long memoryUsedRate = requestMemory.isPresent() && requestMemory.get() != 0 ? avgProcMem * 100 / mb / requestMemory.get() : 0;

    System.out.printf("%-32s%-20s%-20s%-20s%-20s%-20s" + System.getProperty("line.separator"),
            appId,
            avgCpu + "%/" + maxCpu + "%",
            (avgRMem / mb) + "mb/" + (maxRMem / mb) + "mb",
            (avgJvmMem / mb) + "mb/" + (maxJvmMem / mb) + "mb",
            (avgProcMem / mb) + "mb/" + (maxProcMem / mb) + "mb",
            memoryUsedRate + "%"
    );

  }

  private void printDbPhaseResult(String appId) {

//        Optional<Double> avgPhaseRuntime = dbCollector.getAvgPhaseRuntime( appId,"AM_PREPARE");
//        System.out.println("AM_PREPARE   ");
//        AM_PREPARE
//        
//    Optional<Double> avgPhaseRuntime =  dbCollector.getAvgPhaseMemoryUsage( appId,"AM_PREPARE");
//      dbCollector.getAvgPhaseCpuRate(appId,"AM_PREPARE");
//    
  }

  private void printWatchResult(String appId, Configuration conf) {
    WatchResult watchResult;
    try {
      watchResult = WatchResult.getWatchDrsResult(appId, conf);
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println("Can't get " + appId + " Info from ResourceManager.");
      return;
    }

    System.out.println("Status : " + watchResult.getStatus());
    System.out.println("R Containers : " + watchResult.getRunningContainer() + " / " + watchResult.getTotalContainerNumber());
    System.out.println("Progress : " + watchResult.getProgress() + " / " + watchResult.getFileCount());
    String am_Location = "AM Location" + watchResult.getAMnode();
    System.out.println("Lastest Message : " + watchResult.getErrorMessage());

  }

  private void printErrorMessage(Exception ex) {
    ex.printStackTrace();
    System.out.println("failed.(" + ex.getMessage() + ")");
  }

}
