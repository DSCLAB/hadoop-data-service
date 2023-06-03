package com.dslab.drs.simulater;

import com.dslab.drs.restful.api.response.watch.WatchResult;
import com.dslab.drs.simulater.connection.jdbc.ApplicationResourceInfos;
import com.dslab.drs.simulater.connection.jdbc.DbCollector;
import com.dslab.drs.simulater.connection.resourceManager.RmRestfulApiRequestor;
import com.dslab.drs.simulater.connection.resourceManager.RmRestfulApiRequestorImpl;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class SimulatorStatusInfos {

  private static final Log LOG = LogFactory.getLog(SimulatorStatusInfos.class);
  private final SimulatorConfManager confManager;
  private final String rmHttpAddressPort;
  private final DbCollector dbCollector;

  private final RmRestfulApiRequestor rmRestfulApiResquestor;

  final ApplicationInfos applicationInfos;

  private final String lastApplicationId;

  public SimulatorStatusInfos(SimulatorConfManager confManager, String rmHttpAddressPort) throws IOException {
    this.confManager = confManager;
    this.dbCollector = DbCollector.getDbCollector(confManager.getDbConf());
    this.rmRestfulApiResquestor = new RmRestfulApiRequestorImpl();
    this.rmHttpAddressPort = rmHttpAddressPort;

    //紀錄執行前的Container狀態
    Optional<String> getedLastApplicationId = rmRestfulApiResquestor.getLastApplicationId(rmHttpAddressPort);
    String lastApplicationId = getedLastApplicationId.isPresent() ? getedLastApplicationId.get() : "application_0_0";
    this.lastApplicationId = lastApplicationId;
    LOG.info("lastApplicationId:" + lastApplicationId);

    this.applicationInfos = new ApplicationInfos();

  }

  public void updataStatus() throws SQLException, IOException {
    //以下是每次讀取要更新的
    Set<String> afterAppIds = rmRestfulApiResquestor.getAfterAppId(rmHttpAddressPort, lastApplicationId);
    Set<String> runningAppIds = rmRestfulApiResquestor.getRunningAppId(rmHttpAddressPort);

    LOG.debug("Launched" + afterAppIds.size());
    LOG.debug("Running" + runningAppIds.size());

    applicationInfos.setAfterAppIds(afterAppIds);
    applicationInfos.setRunningAppIds(runningAppIds);

    for (String appId : afterAppIds) {

      Optional<ApplicationResourceInfos> applicationResourceInfos = dbCollector.getApplicationResourceInfos(appId);
      applicationInfos.addApplicationResourceInfos(appId, applicationResourceInfos);

      WatchResult watchResult = WatchResult.getWatchDrsResult(appId, confManager.getSimulaterConf());
      applicationInfos.setWatchResult(appId, watchResult);

      Optional<Long> requestMemory = dbCollector.getRequestMemory(appId);
      if (requestMemory.isPresent()) {
        applicationInfos.setRequestMemory(appId, requestMemory.get());
      }
    }
  }

  public void printApplicationStatus() {

    System.out.println("Running " + applicationInfos.getRunningAppIds().size() + " jobs, Started " + applicationInfos.getAfterAppIds().size() + " jobs.");
    System.out.println();

    String appIdTitle = "ApplicationId";
    String launchedContainerTitle = "R Containers";
    String cpuTitle = "Cpu(Avg/Max)";
    String r_MemTitle = "R Mem(Avg/Max)";
    String jvm_MemTitle = "Jvm Mem(Avg/Max)";
    String proc_MemTitle = "Proc Mem(Avg/Max)";
    String prc_MemRateTitle = "Avg Mem Used Rate";

    String watch_ProgressTitle = "Progress";
    String watch_StatusTitle = "Status";
    String am_Location = "AM Location";
    String error_MessageTitle = "Lastest Message";

    System.out.printf("%-32s%-20s%-20s%-20s%-20s%-20s%-20s%-20s%-20s%-20s%-20s" + System.getProperty("line.separator"), appIdTitle, launchedContainerTitle, cpuTitle, r_MemTitle, jvm_MemTitle, proc_MemTitle, prc_MemRateTitle, watch_ProgressTitle, watch_StatusTitle, am_Location, error_MessageTitle);

    for (String appId : applicationInfos.getAfterAppIds()) {
      Optional<ApplicationResourceInfos> applicationResourceInfos = applicationInfos.getApplicationResourceInfos(appId);
      if (applicationResourceInfos.isPresent()) {
        long mb = 1024 * 1024;
        long requestMemory = applicationInfos.getRequestMemory(appId).isPresent() ? applicationInfos.getRequestMemory(appId).get() : 0;
        long memoryUsedRate = requestMemory != 0 ? applicationResourceInfos.get().getAvgProcMem() * 100 / mb / requestMemory : 0;
        Optional<WatchResult> watchResult = applicationInfos.getWatchResult(appId);

        System.out.printf("%-32s%-20s%-20s%-20s%-20s%-20s%-20s%-20s%-20s" + System.getProperty("line.separator"),
                appId,
                watchResult.isPresent()
                        ? watchResult.get().getRunningContainer() + "/" + watchResult.get().getTotalContainerNumber()
                        : "0/0",
                applicationResourceInfos.get().getAvgCpu() + "%/" + applicationResourceInfos.get().getMaxCpu() + "%",
                (applicationResourceInfos.get().getAvgRMem() / mb) + "mb/" + (applicationResourceInfos.get().getMaxRMem() / mb) + "mb",
                (applicationResourceInfos.get().getAvgJvmMem() / mb) + "mb/" + (applicationResourceInfos.get().getMaxJvmMem() / mb) + "mb",
                (applicationResourceInfos.get().getAvgProcMem() / mb) + "mb/" + (applicationResourceInfos.get().getMaxProcMem() / mb) + "mb",
                memoryUsedRate + " %",
                watchResult.isPresent()
                        ? watchResult.get().getProgress() + " / " + watchResult.get().getFileCount()
                        : "-",
                watchResult.isPresent() ? watchResult.get().getStatus() : "-",
                watchResult.isPresent() ? watchResult.get().getStatus() : "-",
                watchResult.isPresent() ? watchResult.get().getErrorMessage() : "-"
        );
      } else {
        System.out.printf("%-32sno query result" + System.getProperty("line.separator"), appId);
      }
    }

    System.out.println();
  }

  public void printTotalCalulatedInfos() {

    System.out.println("Total Files : " + SimulatorShareObject.getTotalThreadInputFileNumber());
    int i = 1;
    int multiple = confManager.getMultiple();
    for (String num : confManager.getRequestContainerSizeSet().split(",")) {
      System.out.println("Request memory : " + confManager.getIntervalMinMemSize() * i + " mb , Number = " + num);
      i = multiple * i;
    }
    System.out.println("Request vcores : " + confManager.getRequestContainerVcores());
    System.out.println("Success rate : " + applicationInfos.getTotalWatchProgress() + " / " + applicationInfos.getTotalWatchFileCount() + " (" + applicationInfos.getFileSuccessRateString() + " %)");

    Optional<Timestamp> startTimeStamp = applicationInfos.getWatchEarliestStartTimestamp();
    String startTime = startTimeStamp.isPresent() ? startTimeStamp.get().toString() : "-1";
    System.out.println("Start time : " + startTime);

    Optional<Long> elapsedTimeMs = applicationInfos.getElapsedTimeMs();
    double elapseTimeSecond = -1;
    if (elapsedTimeMs.isPresent()) {
      elapseTimeSecond = ((double) elapsedTimeMs.get()) / 1000d;
    }
    System.out.println("Elapsed time : " + elapseTimeSecond + " seconds");

    double totalWatchElapsed = applicationInfos.getTotalWatchElapsedTimeMs() / 1000;
    System.out.println("Total Watch Elapsed time : " + totalWatchElapsed + " seconds");

    System.out.println("Throughtput : " + applicationInfos.getThroughput() + "  file / seconds");
  }

}
