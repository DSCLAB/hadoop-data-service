package com.dslab.drs.yarn.application;

import com.dslab.drs.tasks.LocalityScheduler;
import com.dslab.drs.tasks.Scheduler;
import com.dslab.drs.tasks.DefaultScheduler;
import com.dslab.drs.api.NodeStatus;
import com.dslab.drs.api.ServiceStatus;
import com.dslab.drs.api.async.AMRMClientAsync;
import com.dslab.drs.api.async.AMRMClientAsync.CallbackHandler;
import com.dslab.drs.exception.ApplicationMasterException;
import com.dslab.drs.exception.DispatchResourceNotExistException;
import com.dslab.drs.exception.GetJsonBodyException;
import com.dslab.drs.exception.UploadNecessaryFilesException;
import com.dslab.drs.restful.api.response.node.NodeResult;
import com.dslab.drs.restful.api.response.node.NodesBody;
import com.dslab.drs.restful.api.response.node.NodesResult;
import com.dslab.drs.restful.api.parser.Parser;
import com.dslab.drs.hdsclient.HdsClient;
import java.awt.EventQueue;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
//import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
//import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import com.dslab.drs.container.DrsUdfManager;
import com.dslab.drs.drslog.DrsPhaseLog;
import com.dslab.drs.drslog.DrsPhaseLogHelper;
import com.dslab.drs.drslog.LogServerManager;
import com.dslab.drs.drslog.LogServerManagerImpl;
import static com.dslab.drs.monitor.MonitorConstant.*;
import com.dslab.drs.utils.ProcessTimer;
import com.dslab.drs.restful.api.json.JsonUtils;
import com.dslab.drs.socket.SocketGlobalVariables;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.yarn.application.containermanager.ContainerInterval;
import com.dslab.drs.yarn.application.containermanager.ContainerIntervalException;
import com.dslab.drs.yarn.application.containermanager.ContainerManagerImpl;
import com.dslab.drs.yarn.application.containermanager.ContainerResizeArgs;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.log4j.Logger;

/**
 *
 * @author CDCLAB
 */
public final class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  private static LogServerManager logServer;

  private Scheduler schedule;
  private static final DrsConfiguration drsConfiguration = DrsConfiguration.newDrsConfiguration();
  private AMRMClientAsync<ContainerRequest> amRMClient;
//  int lastAskForContainerNumber = 0;
//  int lastProgress = 0;

  // 此變數紀錄Contianer 正常關閉、不預期關閉、被kill 總次數
  protected AtomicInteger numCompletedContainers = new AtomicInteger();
  private volatile boolean done;   //設為 valatile 提醒 JVM 在使用時取得最新值
  protected NMClientAsync nmClient;

  protected HdsClient hdsClient;

  private final NMCallbackHandler containerListener;
  private List<Thread> launchThreads = new ArrayList<>();

  private long applicationStartTime = System.currentTimeMillis();

  private String share_account;
  private FileSystem fs;
  private String final_status;

//  protected Map<ContainerId, NodeId> ContainerMapNodeID = new HashMap<>();  //save launched containerId and NodeId
//  public Map<String, ContainerId> launchContainerStringID = new HashMap<>(); //save launched containerId.toString()
  Set<String> preemptionContainerIDs = new HashSet<>();
  Set<String> strictPreemptionContainerIDs = new HashSet<>();

  public List<String> hostList = new ArrayList<>();        //save slaves name
  public List<String> dispatchResourceList = new ArrayList<>();  //save dispatchResource name

  public Set<String> runningContainerID = new HashSet<>(); //save launched containerId.toString()
  private Set<String> timeoutContainerID = new HashSet<>(); //save launched containerId.toString()

  Map<String, String> env = System.getenv();
  Map<String, String> container_env = new HashMap<>();

  String HDS_ACCESS_ADDRESS;
  String DRS_CONFIG_LOCATION;
  String CLIENT_HOST_ADDRESS;

  String R_CODE;
  String DATA;
  String CONFIG;
  String CODEOUT;
  String COPYTO;
  String CONSOLEOUT;

  String APPID;
  String APPNAME;

  /////////////////////////////////////////////////
  ConcurrentHashMap<String, List<String>> requestTable = new ConcurrentHashMap<>();
  ConcurrentHashMap<String, List<String>> containerTable = new ConcurrentHashMap<>();
  SocketServer socketServer;
  int CONTAINER_FIRST_REQUEST_NUMBER;
  long CONTAINER_MAX_SUPEND_TIME;
  int CONTAINER_MAX_RETRY_TIME;
  private final AtomicInteger OOMTimes = new AtomicInteger(0);
  private final AtomicInteger preemptionTimes = new AtomicInteger(0);

  private final static ContainerManagerImpl CONTAINER_MANAGER = ContainerManagerImpl.getInstance();
  private String errMsg = "";

  public ApplicationMaster(String[] args) throws IOException {
    try {
      LOG.info("IP:" + Inet4Address.getLocalHost().getHostAddress());
    } catch (UnknownHostException ex) {
      LOG.warn("Get Local Host fail " + ex.getMessage());
    }
    CONTAINER_MANAGER.setConf(drsConfiguration);
    env.keySet().stream().forEach(k -> container_env.put(k, env.get(k)));

    HDS_ACCESS_ADDRESS = env.get("HDS_ACCESS_ADDRESS");
    DRS_CONFIG_LOCATION = env.get("DRS_CONFIG_LOCATION");
    CLIENT_HOST_ADDRESS = env.get("CLIENT_HOST_ADDRESS");
    LOG.info("CLIENT_HOST_ADDRESS:" + CLIENT_HOST_ADDRESS);

    LOG.info("HDS_ACCESS_ADDRESS:" + HDS_ACCESS_ADDRESS);
    LOG.info("DRS_CONFIG_LOCATION:" + DRS_CONFIG_LOCATION);

    R_CODE = env.get("R_CODE");;
    DATA = env.get("DATA");
    CONFIG = env.get("CONFIG");
    CODEOUT = env.get("CODEOUT");
    COPYTO = env.get("COPYTO");
    CONSOLEOUT = env.get("CONSOLEOUT");
    APPID = env.get("APPID");
    APPNAME = env.get("APPNAME");
    LOG.info("R_CODE :" + R_CODE);
    LOG.info("DATA :" + DATA);
    LOG.info("CONFIG :" + CONFIG);
    LOG.info("CODEOUT :" + CODEOUT);
    LOG.info("COPYTO :" + COPYTO);
    LOG.info("CONSOLEOUT :" + CONSOLEOUT);
    LOG.info("APPID :" + APPID);
    LOG.info("APPNAME :" + APPNAME);

    //可透過 Environment、ApplicationConstants 取得YARN的環境變數
    String containerId = env.get(Environment.CONTAINER_ID.name());
    LOG.info("Environment.CONTAINER_ID.name():" + Environment.CONTAINER_ID.name());
    LOG.info("containerId:" + containerId);

    ContainerGlobalVariable.APPLICATION_NAME = APPNAME;
    ContainerGlobalVariable.CONTAINER_ID = containerId;

    container_env.put("COPYTO", COPYTO.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
    if (CONSOLEOUT != null) {
      container_env.put("CONSOLEOUT", CONSOLEOUT.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
    }
    container_env.put("APPID", APPID);
    container_env.put("APPNAME", APPNAME);

    this.hdsClient = new HdsClient(HDS_ACCESS_ADDRESS);
    LOG.info("new hdsclient access address:" + HDS_ACCESS_ADDRESS);

    this.fs = FileSystem.get(new YarnConfiguration());
    LOG.info("addResourceFromHDFS from " + DRS_CONFIG_LOCATION);
    LOG.info("CONFIG:" + CONFIG);

    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    } catch (final Error e) {
      LOG.info("Error in setURLStreamHandlerFactory: catch and continue" + e.getMessage());
    }

    drsConfiguration.set(DATA, DATA);;
    drsConfiguration.addResource(new URL(DRS_CONFIG_LOCATION));
    drsConfiguration.addResource(new Path(CONFIG));
    DrsConfiguration.setDrsDefaultValue(drsConfiguration);
    DrsConfiguration.replaceHdsConfHost(drsConfiguration, CLIENT_HOST_ADDRESS);

    LOG.info("Log level : " + drsConfiguration.getLogLever().toString());
    LOG.info("Scheduler type : " + drsConfiguration.get(DrsConfiguration.DISPATCH_SCHEDULER, DrsConfiguration.DISPATCH_SCHEDULER_DEFAULT));
    Logger.getLogger("com.dslab.drs").setLevel(drsConfiguration.getLogLever());

    LOG.info("Get \"" + DrsConfiguration.YARN_RESOURCEMANAGER_WEB_ADDRESS + "\":" + drsConfiguration.get(DrsConfiguration.YARN_RESOURCEMANAGER_WEB_ADDRESS));
//    LOG.info("Get \"" + DrsConfiguration.YARN_RESOURCEMANAGER_HA_RM_IDS + "\":" + HA_IDs);
    LOG.info("Get \"" + DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS + "\":" + drsConfiguration.get(drsConfiguration.YARN_RESOURCEMANAGER_ADDRESS));
    LOG.info("Get \"yarn.resourcemanager.webapp.address.rm289\":" + drsConfiguration.get("yarn.resourcemanager.webapp.address.rm289"));
    LOG.info("Get \"yarn.resourcemanager.webapp.address.rm237\":" + drsConfiguration.get("yarn.resourcemanager.webapp.address.rm237"));

    LOG.info("Get \"" + "Container list" + "\":" + drsConfiguration.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST));
//    LOG.info("Get \"" + DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN + "\":" + drsConfiguration.getInt(DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN, DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN_DEFAULT));

    LOG.info("Get \"" + DrsConfiguration.HDS_LOGGER_ENABLE + "\":" + drsConfiguration.get(DrsConfiguration.HDS_LOGGER_ENABLE, DrsConfiguration.HDS_LOGGER_ENABLE_DEFAULT));
    LOG.info("Get \"" + DrsConfiguration.HDS_LOGGER_JDBC_URL + "\":" + drsConfiguration.get(DrsConfiguration.HDS_LOGGER_JDBC_URL));
    LOG.info("Get \"" + DrsConfiguration.HDS_LOGGER_JDBC_DRIVER + "\":" + drsConfiguration.get(DrsConfiguration.HDS_LOGGER_JDBC_DRIVER));

    LOG.info("Get \"" + DrsConfiguration.DRS_SCHEDULER_BATCH_SIZE + "\":" + drsConfiguration.getInt(DrsConfiguration.DRS_SCHEDULER_BATCH_SIZE, DrsConfiguration.DRS_SCHEDULER_BATCH_SIZE_DEFAULT));

    this.share_account = drsConfiguration.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT);

    //讀取slave's node name
    String slaves = drsConfiguration.get(DrsConfiguration.DRS_SLAVES);
    LOG.debug(slaves);
    String[] temp_slave = slaves.split(";");
    for (String temp_slave1 : temp_slave) {
      if (!temp_slave1.equals("")) {
        hostList.add(temp_slave1);
      }
    }

    R_CODE = env.get("R_CODE");;
    DATA = env.get("DATA");
    CONFIG = env.get("CONFIG");
    CODEOUT = env.get("CODEOUT");
    COPYTO = env.get("COPYTO");
    APPID = env.get("APPID");

    //讀取dispatch.resource，將共享資源傳至hdfs。
    String dispatchResource = drsConfiguration.get("dispatch.resource");

    LOG.info("Get \"dispatchResource\":" + dispatchResource);
    if (dispatchResource != null) {
      String[] temp_dispatchResources = dispatchResource.split(";");
      HdsClient hdsClient = new HdsClient(drsConfiguration);
      for (String temp_dispatchResource : temp_dispatchResources) {
        if (!temp_dispatchResource.equals("")) {
          String path = temp_dispatchResource;
          String fileName = FilenameUtils.getName(path);
          dispatchResourceList.add(path);
          String pathSuffix = "/" + DrsConfiguration.APP_NAME + "/" + APPID + "/" + fileName;
          try {
            hdsClient.hdsAccessService(path, share_account + pathSuffix);
          } catch (UploadNecessaryFilesException ex) {
            LOG.info("dispatchResource hdsAccessService address:" + ex.getMessage());
          }
        }
      }
      checkAndAccessUDF();
    }

    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
    amRMClient.init(drsConfiguration);
    amRMClient.start();
    CONTAINER_MANAGER.setAMRMClient(amRMClient);
    try {
      checkDispatchResourceExist(drsConfiguration, dispatchResourceList);
    } catch (DispatchResourceNotExistException ex) {
      throw new ApplicationMasterException(ex.getMessage());
    }
    try {
      RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");
      LOG.info("Response of register to RM:" + response.toString());
    } catch (YarnException | IOException ex) {
      LOG.fatal(ex);
    }

    //start nmclient implement NMCallbackHandler
    containerListener = new NMCallbackHandler(this);
    nmClient = NMClientAsync.createNMClientAsync(containerListener);
    nmClient.init(drsConfiguration);
    nmClient.start();
    CONTAINER_MANAGER.setNMClient(nmClient);
  }

//  private int getContainerRequestMemory() {
//    return drsConfiguration.getInt(
//            DrsConfiguration.CONTAINER_MEMORY_MB,
//            DrsConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
//  }
  private int getContainerRequestVCores() {
    return drsConfiguration.getInt(
            DrsConfiguration.CONTAINER_CPU_VCORES,
            DrsConfiguration.CONTAINER_CPU_VCORES_DEFAULT);
  }

  private void checkAndAccessUDF() {
    boolean result = false;
    DrsUdfManager udfmanager = new DrsUdfManager(drsConfiguration);
    udfmanager.init();
    LOG.debug("hasUdfBeforeCode  :" + udfmanager.hasUdfBeforeCode());
    LOG.debug("isUdfBeforeCodeFromRemote  :" + udfmanager.isUdfBeforeCodeFromRemote());
    LOG.debug("hasUdfAfterCode  :" + udfmanager.hasUdfAfterCode());
    LOG.debug("isUdfAfterCodeFromRemote  :" + udfmanager.isUdfAfterCodeFromRemote());

    if (udfmanager.hasUdfBeforeCode()) {
      String fileName = udfmanager.getBeforeFileName();
      String address = udfmanager.getBeforeAddress();
      if (udfmanager.isUdfBeforeCodeFromRemote()) {
        String pathSuffix = "/" + DrsConfiguration.APP_NAME + "/" + APPID + "/" + fileName;
        try {
          //hds get file into hdfs share account path
          hdsClient.hdsAccessService(address, share_account + pathSuffix);
        } catch (UploadNecessaryFilesException ex) {
          LOG.info("Udf hdsAccessService address:" + ex.getMessage());
        }
        dispatchResourceList.add(address);
      } else {
        try {
          String pathSuffix = "/user/yarn/" + DrsConfiguration.APP_NAME + "/" + APPID + "/" + fileName;
          FileSystem fs = FileSystem.get(drsConfiguration);
          FileUtil.copy(fs, new Path(address), fs, new Path(pathSuffix), false, drsConfiguration);
          address = "hdfs://".concat(address);
          dispatchResourceList.add(address);
        } catch (Exception ex) {
          LOG.error("DrsManager encounter error :" + ex.toString());
          LOG.error("DrsManager encounter error :" + ex.getMessage());
          ex.printStackTrace();
        }
      }
    }

    if (udfmanager.hasUdfAfterCode()) {
      String fileName = udfmanager.getAfterFileName();
      String address = udfmanager.getAfterAddress();
      if (udfmanager.isUdfAfterCodeFromRemote()) {
        String pathSuffix = "/" + DrsConfiguration.APP_NAME + "/" + APPID + "/" + fileName;
        LOG.debug(pathSuffix);
        try {
          //hds get file into hdfs share account path
          hdsClient.hdsAccessService(address, share_account + pathSuffix);
        } catch (UploadNecessaryFilesException ex) {
          LOG.info("Udf hdsAccessService address:" + ex.getMessage());
        }
        dispatchResourceList.add(address);
      } else {
        try {
          String pathSuffix = "/user/yarn/" + DrsConfiguration.APP_NAME + "/" + APPID + "/" + fileName;
          FileSystem fs = FileSystem.get(drsConfiguration);
          FileUtil.copy(fs, new Path(address), fs, new Path(pathSuffix), false, drsConfiguration);
          address = "hdfs://".concat(address);
          dispatchResourceList.add(address);
        } catch (Exception ex) {
          LOG.error("DrsManager encounter error :" + ex.toString());
          LOG.error("DrsManager encounter error :" + ex.getMessage());
          ex.printStackTrace();
        }
      }
    }
    LOG.debug("checkAndAccessUDF end:");
  }

  //AM run
  public void run() throws ApplicationMasterException, ContainerIntervalException {
    LOG.info("Start ApplicationMaster.run()");
    Long start_time = System.currentTimeMillis();

    ProcessTimer timer = ProcessTimer.getTimer();
    //set prepare time
    long prepare_dif = timer.getDifferenceMillis();
    DrsPhaseLog prepareLog = DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(), ContainerGlobalVariable.APPLICATION_NAME, ContainerGlobalVariable.CONTAINER_ID, null, AM_PREPARE, prepare_dif);
    logServer.addPhaseLog(prepareLog);

    //set cCores 
//    long memoryRequest = (long) getContainerRequestMemory();
    DrsPhaseLog memoryLog = DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(), ContainerGlobalVariable.APPLICATION_NAME, ContainerGlobalVariable.CONTAINER_ID, null, MEMORY_REQUEST, 1024);
    logServer.addPhaseLog(memoryLog);

    //set cCores 
    long vCoresRequest = (long) getContainerRequestVCores();
    DrsPhaseLog cCoreLog = DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(), ContainerGlobalVariable.APPLICATION_NAME, ContainerGlobalVariable.CONTAINER_ID, null, CORES_REQUEST, vCoresRequest);
    logServer.addPhaseLog(cCoreLog);

    ContainerGlobalVariable.RESOURCE_LOG_TYPE = AM_LIST;

    long startTime = System.currentTimeMillis();

    LOG.debug("input hosts:" + hostList);
    LOG.info("Start Schedule...");
    settingContainerInterval();
    String hds_list_address = drsConfiguration.get(
            DrsConfiguration.DISPATCH_HDS_LIST_ADDRESS,
            DrsConfiguration.DISPATCH_HDS_LIST_ADDRESS_DEFAULT);
    try {
      if ("locality".equals(drsConfiguration.get(DrsConfiguration.DISPATCH_SCHEDULER, DrsConfiguration.DISPATCH_SCHEDULER_DEFAULT))) {
        schedule = new LocalityScheduler(hostList, hds_list_address, DATA, drsConfiguration);
        if (schedule instanceof LocalityScheduler) {
          schedule.init();
          CONTAINER_MANAGER.setScheduler(schedule);
        }
      } else {
        schedule = new DefaultScheduler(hostList, hds_list_address, DATA, drsConfiguration);
        if (schedule instanceof DefaultScheduler) {
          schedule.init();
          CONTAINER_MANAGER.setScheduler(schedule);
        }
      }
    } catch (Exception e) {
      LOG.info("HDS" + e.toString());
      LOG.info("HDS list schedule fail");
      e.printStackTrace();
      throw new ApplicationMasterException("ApplicationMasterException:Error when list " + DATA + " infomation.");
    }
    long endTime = System.currentTimeMillis();

    long list_dif = timer.getDifferenceMillis();

    DrsPhaseLog listLog = DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(), ContainerGlobalVariable.APPLICATION_NAME, ContainerGlobalVariable.CONTAINER_ID, null, AM_LIST, list_dif);
    logServer.addPhaseLog(listLog);

    ContainerGlobalVariable.RESOURCE_LOG_TYPE = AM_SCHEDULING;

    LOG.debug("start wait for tasks");
    while (!SocketGlobalVariables.getAMGlobalVariable().isFirstListEnd()) {
      try {
//        LOG.debug("Batch Acc = " + SocketGlobalVariables.getAMGlobalVariable().getBatchAcc());
        LOG.debug("Waiting for first batch task 1isted.");
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException ex) {
      }
    }

    LOG.debug("first batch finished!");
    LOG.debug("schedule file number = " + schedule.getTaskNumber());
    if (schedule.getTaskNumber() == 0) {
      throw new ApplicationMasterException("ApplicationMasterException:Inputfile '" + DATA + "' has no available url,please check this.");
    }

    LOG.info("Start socketServer!");
    String sharePath = share_account + "/" + DrsConfiguration.APP_NAME + "/" + APPID;

    socketServer = new SocketServer(schedule, fs, sharePath, applicationStartTime, drsConfiguration);
    socketServer.globals.setContainerMng(CONTAINER_MANAGER);
    socketServer.globals.getServiceStatus().setApplicationID(APPNAME);
    socketServer.start();

//    List<ContainerRequestBody> containerRequestList = createContainerRequestList(drsConfiguration);
//
//    int applicationLaunchContainerNumberMin = drsConfiguration.getInt(
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN,
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN_DEFAULT);
//
//    if (containerRequestList.size() < applicationLaunchContainerNumberMin) { //if after compute the available resource is not enough , AM will stop
//      throw new ApplicationMasterException("Can't allocate enough resource, you expects at least " + applicationLaunchContainerNumberMin + ", but we only allow " + containerRequestList.size());
//    }
    List<ContainerRequestBody> containerRequestList = createContainerRequestList(drsConfiguration);
    LOG.info("AMExpectedContainer size = " + containerRequestList.size());
    SocketGlobalVariables.getAMGlobalVariable().setAMExpectedContainer(containerRequestList.size());

    containerRequestList.stream().forEach(
            containerRequestBody -> CONTAINER_MANAGER.request(containerRequestBody.getNodeHostName(),
                    DrsConfiguration.RELAX_LOCALITY_DEFAULT, containerRequestBody.getMemoryMB(), getContainerRequestVCores()));
    socketServer.globals.setFirstRequestTime(System.currentTimeMillis());
//    for (ContainerRequestBody containerRequestBody : containerRequestList) {
//      String localhost = containerRequestBody.getNodeHostName();
//      askForContainer(localhost, DrsConfiguration.RELAX_LOCALITY_DEFAULT, containerRequestBody.getMemoryMB());
//    }

    //TODO request fail 等待五秒
    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (Exception e) {

    }

    int count = 0;
//    int loopCount = 0;

    LOG.info("Start Monitor container status.");
    String appMessage = null;

    //CONTAINER_FIRST_REQUEST_NUMBER = 一開始就訂好的 Container數量 ，不會再變。
    CONTAINER_FIRST_REQUEST_NUMBER = containerRequestList.size();
    CONTAINER_MAX_RETRY_TIME = drsConfiguration.getInt(
            DrsConfiguration.CONTAINER_MAX_RETRY_TIMES,
            DrsConfiguration.CONTAINER_MAX_RETRY_TIMES_DEFAULT);
    CONTAINER_MAX_SUPEND_TIME = drsConfiguration.getLong(
            DrsConfiguration.CONTAINER_MAX_SUSPEND_TIME_SEC,
            DrsConfiguration.CONTAINER_MAX_SUSPEND_TIME_SEC_DEFAULT) * 1000;

    LOG.info("\"" + DrsConfiguration.CONTAINER_MAX_RETRY_TIMES + "\":" + CONTAINER_MAX_RETRY_TIME);
    LOG.info("\"" + DrsConfiguration.CONTAINER_MAX_SUSPEND_TIME_SEC + "\":" + CONTAINER_MAX_SUPEND_TIME);

//    printAmStatus();
//    LOG.info(loopCount
//            + ",Progress = " + schedule.getProgress()
//            + ",fileNumber= " + schedule.getTaskNumber()
//            + ",The number of completed Containers = " + this.numCompletedContainers.get() + " / " + askForContainerNumber);
    int printAmStatusCount = 0;
    int delay = 0;
    String policy = drsConfiguration.get(
            DrsConfiguration.DRS_CONTAINERMANAGER_RESIZE_POLICY,
            DrsConfiguration.DRS_CONTAINERMANAGER_RESIZE_POLICY_DEFAULT).toUpperCase();
    boolean resizeEnable = Boolean.parseBoolean(drsConfiguration.get(
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_CONTAINER_RESIZE_ENABLE,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_CONTAINER_RESIZE_ENABLE_DEFAULT));

    int cycleTime = drsConfiguration.getInt(
            DrsConfiguration.DRS_CONTAINERMANAGER_CYCLIC_POLICY_CYCLETIME_MS,
            DrsConfiguration.DRS_CONTAINERMANAGER_CYCLIC_POLICY_CYCLETIME_MS_DEFAULT);
    LOG.info("policy = " + policy);
    while (!done
            && (SocketGlobalVariables.getAMGlobalVariable().getAMExpectedContainer() > 0)
            //            && isAllContainerComplete()
            && socketServer.isServerRunning()
            && !socketServer.isServerKilled()) {
      printAmStatusCount++;
      if (printAmStatusCount % 10 == 0) {
        LOG.info("AMExpected container = " + SocketGlobalVariables.getAMGlobalVariable().getAMExpectedContainer());
        printAmStatus();
      }

      if (delay > 15 && resizeEnable) {
//        LOG.debug("delay = " + delay);
        if (policy.equals("CYCLICAL") && (printAmStatusCount % (cycleTime / 1000) == 0)) {
          CONTAINER_MANAGER.startResizeContainerAlgo(policy);
        } else if (policy.equals("DONETASK")) {
          CONTAINER_MANAGER.startResizeContainerAlgo(policy);
        } else if (policy.equals("DEFAULT")) {
          CONTAINER_MANAGER.startResizeContainerAlgo(policy);
        }
      }
      delay++;

      //確認是否  還沒執行結束，卻結束。
      // 目前container要執行數量是否為預期數量      
      if (isRunningContainerUnderExpected()) {
        //schedule.isComplete mean all tasks done,not scheduled done.

//        if (!schedule.isComplete()) {
//        recoveryUnexpectContainer();
        recoveryUnExpectedContainer();
        continue;
//        }
      }

      ckeckAndRecoveryTimeoutContainer();

      try {
        TimeUnit.MILLISECONDS.sleep(DrsConfiguration.AM_CHECK_INTERVAL_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }  //end while
    LOG.info("AMExpected container = " + SocketGlobalVariables.getAMGlobalVariable().getAMExpectedContainer());
    printAmStatus();
    printAmDebugMessage();

    LOG.info("----done------");
    socketServer.closeServerSucket();

    LOG.info("Last The number of completed Containers = " + this.numCompletedContainers.get() + " / " + CONTAINER_MANAGER.getAskContainerNum());
    //    amRMClient.removeContainerRequest(contianer request);

    socketServer.globals.getServiceStatus().updateNodesStatus(schedule.getNodesStatus(), schedule.getProgress());
    socketServer.globals.getServiceStatus().setIsComplete(schedule.isComplete());

    Long end_time = System.currentTimeMillis();

    LOG.info("Total time=" + (new Double(end_time - start_time)) / 1000);
    LOG.info("end of run!");
  }

  private void settingContainerInterval() throws ContainerIntervalException {
    int intervalNum = drsConfiguration.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_COUNT,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_COUNT_DEFAULT);
    int minSize = drsConfiguration.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB_DEFAULT);
    int multiple = drsConfiguration.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE_DEFAULT);
    String intervalBoundary;

    if (drsConfiguration.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_BOUNDARY) == null) {
      intervalBoundary = "0,-1";
      LOG.info("container interval boundary = " + intervalBoundary);
    } else {
      intervalBoundary = "0," + drsConfiguration.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_BOUNDARY) + ",-1";
      LOG.info("container interval boundary = " + intervalBoundary);
    }
    String containerNumSet = drsConfiguration.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST_DEFAULT);

    if (intervalBoundary.trim().split(",").length != (intervalNum + 1)
            || containerNumSet.trim().split(",").length != intervalNum) {
      LOG.info("bounary =" + intervalBoundary.trim().split(",").length);
      LOG.info("containerNumSet =" + (containerNumSet.trim().split(",").length + 1));
      throw new ContainerIntervalException("container manager configuration setting wrong!");
    }

    Map<Integer, ContainerInterval> containerIntervals
            = ContainerManagerImpl.parseToContainerIntervals(intervalNum, minSize,
                    multiple, intervalBoundary.trim(), containerNumSet);
    CONTAINER_MANAGER.putAllContainerInterval(containerIntervals);
  }

  private boolean isAllContainerComplete() {
    return numCompletedContainers.get() < CONTAINER_MANAGER.getAskContainerNum();
  }

  private boolean isRunningContainerUnderExpected() {
    return getRunningContainerNumber() < SocketGlobalVariables.getAMGlobalVariable().getAMExpectedContainer();
  }

  private int getRunningContainerNumber() {
    return CONTAINER_MANAGER.getAskContainerNum() - numCompletedContainers.get();
  }

  private void printAmStatus() {
    LOG.info("Progress = " + schedule.getProgress()
            + ",Queue size= " + schedule.getTaskNumber()
            + ",Total fileNumber= " + schedule.getTaskAccNumber()
            + ",The number of completed Containers = " + this.numCompletedContainers.get() + " / " + CONTAINER_MANAGER.getAskContainerNum());
  }

  private void printAmDebugMessage() {
    LOG.info("!done:" + (!done));
    LOG.info("AMExpectedContainer > 0 :" + (SocketGlobalVariables.getAMGlobalVariable().getAMExpectedContainer() > 0));
    LOG.info("socketServer.isServerRunning():" + socketServer.isServerRunning());
    LOG.info("!socketServer.isServerKilled():" + (!socketServer.isServerKilled()));
    LOG.info("success task number = " + schedule.getProgress());
    LOG.info("remaining task number = " + schedule.getTaskNumber());
  }

  private void recoveryUnexpectContainer() {
    LOG.info("One Container unexpect complete. Start another new Container.");
    if (CONTAINER_MANAGER.getAskContainerNum() >= CONTAINER_FIRST_REQUEST_NUMBER + CONTAINER_MAX_RETRY_TIME + preemptionTimes.get()) {   //失敗太多次  強制結束
      LOG.info("Too Many Container failed! fail times,Total Container numbers:" + (CONTAINER_FIRST_REQUEST_NUMBER + CONTAINER_MAX_RETRY_TIME));
      final_status = "container retry over limit (" + CONTAINER_MAX_RETRY_TIME + ") ";
      done = true;
    }
    //進入此代表"有節點發生不預期的結束"，重新請求
    CONTAINER_MANAGER.request("", true, 1024, getContainerRequestVCores());
  }

  private void recoveryUnExpectedContainer() {// CM return intervals info <memSize,isExpected>
    LOG.info("One Container unexpect complete. Start another new Container.");
    if (CONTAINER_MANAGER.getAskContainerNum() >= CONTAINER_FIRST_REQUEST_NUMBER
            + CONTAINER_MAX_RETRY_TIME + preemptionTimes.get() + OOMTimes.get() + CONTAINER_MANAGER.getRequestNum() - CONTAINER_MANAGER.getReleaseNum()) {   //失敗太多次  強制結束
      LOG.info("Too Many Container failed! fail times,Total Container numbers:" + (CONTAINER_FIRST_REQUEST_NUMBER + CONTAINER_MAX_RETRY_TIME));
      final_status = "container retry over limit (" + CONTAINER_MAX_RETRY_TIME + ") ";
      done = true;
    }
    CONTAINER_MANAGER.recoveryUnExpectedContainer().entrySet()
            .stream().filter(entry -> entry.getValue()).forEach(entry
            -> CONTAINER_MANAGER.request("", true, entry.getKey(), getContainerRequestVCores()));
  }

  private void ckeckAndRecoveryTimeoutContainer() {
    //確認是否  "有一個點執行太久"  kill該點並重新要一個
    Long currentTime = System.currentTimeMillis();
    synchronized (runningContainerID) {
      Iterator<String> iterator = runningContainerID.iterator();
      while (iterator.hasNext()) {
        String containerID = iterator.next();

        Long containerSuspendTime = currentTime - socketServer.globals.getContainerLastResponseTime(containerID);
        if (containerSuspendTime > CONTAINER_MAX_SUPEND_TIME) {// nmClient.stopContainerAsync(containerid, ContainerMapNodeID.get(containerid));  //終止Container

          ContainerId containerid = CONTAINER_MANAGER.getContainerID(containerID);
          NodeId nodeId = CONTAINER_MANAGER.getContainerMappingNodeID(containerid);

          //判斷 是否是time out 是因為等待其他節點導致，看該節點有沒有執行中任務
          //socketServer.clearContainerRunningTask(containerID, nodeId.toString().split(":")[0]);
          if (!socketServer.isContainerHasTask(containerID, nodeId.toString().split(":")[0])) {
            continue;   //沒有分給那節點任務，檢查其他人
          }
          LOG.info(containerID + " no response " + containerSuspendTime + "(ms),Timeout!");
          LOG.info("containerSuspendTime:" + containerSuspendTime + ",max = " + CONTAINER_MAX_SUPEND_TIME);
          LOG.info("Try to Kill one container ,and request new one.");
          int askContainerLimit = preemptionTimes.get() + OOMTimes.get()
                  + CONTAINER_MAX_RETRY_TIME + CONTAINER_FIRST_REQUEST_NUMBER
                  + CONTAINER_MANAGER.getRequestNum() - CONTAINER_MANAGER.getReleaseNum();
          if (CONTAINER_MANAGER.getAskContainerNum() >= askContainerLimit) {   //失敗太多次  強制結束 max times are container.max.retry.times
            LOG.info("Too Many Container failed! fail times,Total Container numbers:" + (CONTAINER_FIRST_REQUEST_NUMBER + CONTAINER_MAX_RETRY_TIME));
            final_status = "container retry over limit (" + CONTAINER_MAX_RETRY_TIME + ") ";
            done = true;
            break;
          }

          String runningTask = socketServer.getContainerRunningTasks(containerID);

          LOG.info("Shutting down MyContainer in " + containerID + "(" + runningTask + ",timeout), Container: " + CONTAINER_MANAGER.getContainerID(containerID));
          //刪除該任務，(如果再重新要求時，該container結束關掉，numCompletedContainers會多一個，此迴圈會再進一次)
//          killContainer(containerID);
          CONTAINER_MANAGER.release(containerID);
//          numCompletedContainers.incrementAndGet();   //刪掉還是要標記總結束數量
          //請求新任務
          CONTAINER_MANAGER.request("", true, CONTAINER_MANAGER.getContainerResource(containerID).getMemory(), getContainerRequestVCores());

          runningContainerID.remove(containerID);

          //重新分發該節點擁有的任務
          socketServer.clearContainerRunningTask(containerID, nodeId.toString().split(":")[0], false);
          timeoutContainerID.add(containerID);

          break;    //注意，在Iterator時不該移除物件。
        }
      }
    }

  }

  private static NodesBody getNodesResult(String rmHttpAddressPort) throws GetJsonBodyException {
    String rmHttpNodesPath = "http://" + rmHttpAddressPort + "/ws/v1/cluster/nodes";
    String jsonStr = Parser.getJsonBodyFromUrl(rmHttpNodesPath);
    NodesBody jsonResult = JsonUtils.fromJson(jsonStr, NodesBody.class);
    return jsonResult;
  }

//  private List<ContainerRequestBody> createContainerRequestList(DrsConfiguration conf) throws ApplicationMasterException {
//
//    String HA_enable = conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_HA_ENABLE);
//    String HA_IDs = conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_HA_RM_IDS);
//    List<ContainerRequestBody> containerRequestList = new ArrayList<>();
//
//    if ("true".equals(HA_enable)) {
//      LOG.info("Schedule for HA mode...");
//      if (HA_IDs != null) {
//        String[] rm_ids = HA_IDs.split(",");
//
//        int connectionCount = 0;
//
//        for (String rm_id : rm_ids) {
//          String tempHttpAddressPosrt = conf.get("yarn.resourcemanager.webapp.address." + rm_id);
//          try {
//            return containerRequestList = computeContainerRequestList(tempHttpAddressPosrt);
//          } catch (GetJsonBodyException ex) {
//            connectionCount++;
//            LOG.error("The rm id connect RM fail:" + rm_id + "," + connectionCount + "/" + rm_ids.length + "," + " times");
//            LOG.error(ex.getMessage());
//            if (connectionCount == rm_ids.length) {
//              throw new ApplicationMasterException("Can't connect to any host of yarn.resourcemanager.ha.rm-ids");
//            }
//          }
//        }
//      } else {
//        throw new ApplicationMasterException("The value of yarn.resourcemanager.ha.rm-ids : null");
//      }
//    } else {
//      String rmHttpAddressPort = conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_WEB_ADDRESS);
//      try {
//        containerRequestList = computeContainerRequestList(rmHttpAddressPort);
//      } catch (GetJsonBodyException ex) {
//        throw new ApplicationMasterException("Can't connect to  host of yarn.resourcemanager.webapp.address .");
//      }
//    }
//    return containerRequestList;
//  }
  /**
   * no use method
   */
//  private List<ContainerRequestBody> computeContainerRequestList(String rmHttpAddressPort)
//          throws GetJsonBodyException, ApplicationMasterException {
//    String rmHttpNodesPath = "http://" + rmHttpAddressPort + "/ws/v1/cluster/nodes";
//    LOG.debug("rmHttpNodesPath:" + rmHttpNodesPath);
//    NodesBody nodesResult = getNodesResult(rmHttpAddressPort);
//
//    int applicationLaunchContainerNumber = drsConfiguration.getInt(
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER,
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_DEFAULT);
//
//    int applicationLaunchContainerNumberMin = drsConfiguration.getInt(
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN,
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN_DEFAULT);
//
//    if (nodesResult == null) {
//      LOG.debug("Can't get nodesResult from RM Rest.");
//      throw new GetJsonBodyException("Can't get nodesResult :null");
//    }
//
//    NodesResult nodes = nodesResult.getNodes();
//    if (nodes == null) {
//      throw new GetJsonBodyException("Can't get nodes :null");
//    }
//
//    List<NodeResult> nodeList = nodes.getNodes();
//    if (nodeList == null) {
//      throw new GetJsonBodyException("Can't get nodeList :null");
//    }
//
//    LOG.debug("Node size:" + nodeList.size());
//    for (NodeResult nodeResult : nodeList) {
//      LOG.debug("==========================");
//      LOG.debug("NodeHostName:" + nodeResult.getNodeHostName());
//      LOG.debug("MemoryMB:" + nodeResult.getAvailMemoryMB());
//      LOG.debug("VirtualCores:" + nodeResult.getAvailableVirtualCores());
//      LOG.debug("HealthReport:" + nodeResult.getHealthReport());
//    }
//    LOG.debug("==========================");
//    LOG.debug("CONTAINER_REQUEST_MEMORY:" + getContainerRequestMemory());
//    LOG.debug("CONTAINER_REQUEST_VCORES:" + getContainerRequestVCores());
//
//    List<ContainerRequestBody> containerRequestList = new ArrayList<>();
//
//    int requestNumber = 0;
//    for (NodeResult nodeResult : nodeList) {
//      String nodeHostName = nodeResult.getNodeHostName();
//      int availMemoryMB = nodeResult.getAvailMemoryMB();
//      int availCores = nodeResult.getAvailableVirtualCores();
//
//      while (true) {
//        availMemoryMB = availMemoryMB - getContainerRequestMemory();
//        availCores = availCores - getContainerRequestVCores();
//        if (availMemoryMB >= 0 && availCores >= 0) {
//          requestNumber++;
//        } else {
//          break;
//        }
//      }
//    }
//
//    if (requestNumber < applicationLaunchContainerNumberMin) {
//      throw new ApplicationMasterException("This job need at least " + applicationLaunchContainerNumberMin + " containers,but current YARN can only supply " + requestNumber + ".");
//    }
//    if (requestNumber > applicationLaunchContainerNumber) {
//      requestNumber = applicationLaunchContainerNumber;
//    }
//
//    for (int i = 0; i < requestNumber; i++) {
//      containerRequestList.add(new ContainerRequestBody("", getContainerRequestMemory()));
//    }
//
//    return containerRequestList;
//  }
  private List<ContainerRequestBody> createContainerRequestList(DrsConfiguration conf) {
    int intervalNum = drsConfiguration.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_COUNT,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_COUNT_DEFAULT);
    int minSize = drsConfiguration.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB_DEFAULT);
    int multiple = drsConfiguration.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE_DEFAULT);
    String containerNumSet = drsConfiguration.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST_DEFAULT);
    Map<Integer, Integer> containerPair = ContainerManagerImpl.parseToContainerPair(intervalNum, minSize, multiple, containerNumSet);
    int applicationLaunchContainerNumber = containerPair.values().stream().reduce(0, (x, y) -> x + y);
    LOG.info("minSize=" + minSize + ",containerNumSet=" + containerNumSet);
//    int applicationLaunchContainerNumber = drsConfiguration.getInt(
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER,
//            DrsConfiguration.APPLICATION_LAUNCH_CONTAINER_NUMBER_DEFAULT);
    List<ContainerRequestBody> containerRequestList = new ArrayList<>();
    containerPair.keySet().stream().forEach(
            key -> {
              for (int i = 0; i < containerPair.get(key); i++) {
                containerRequestList.add(new ContainerRequestBody("", key));
              }
            });
    LOG.info("containerRequestList.size = " + containerRequestList.size());
//    for (int i = 0; i < applicationLaunchContainerNumber; i++) {
//      containerRequestList.add(new ContainerRequestBody("", getContainerRequestMemory()));
//    }
    return containerRequestList;
  }

  private void checkDispatchResourceExist(YarnConfiguration yarn_dispatch_conf, List<String> dispatchResourceList)
          throws DispatchResourceNotExistException {
    if (dispatchResourceList.isEmpty()) {
      return;
    }

    List<String> failList = new ArrayList<>();
    StringBuilder errorMessage = new StringBuilder();

    dispatchResourceList.stream().forEach((path) -> {
      try {
        hdsClient.checkHdsFileExist(yarn_dispatch_conf, path);
      } catch (Exception ex) {
        failList.add(path);
      }
    });

    if (failList.size() > 0) {
      errorMessage.append("Access dispatch resource fail:");
      for (int i = 0; i < failList.size(); i++) {
        if (i != 0) {
          errorMessage.append(",");
        }
        errorMessage.append(failList.get(i));
      }
      throw new DispatchResourceNotExistException(errorMessage.toString());
    }
  }

  /**
   * Write the object to a Base64 string.
   */
  private static String objectToString(Serializable o) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(o);
      return org.apache.commons.codec.binary.Base64.encodeBase64String(baos.toByteArray());
    }
  }

//  private void killContainer(String containerID) {
//    ContainerId containerid = CONTAINER_MANAGER.getContainerID(containerID);
//    NodeId nodeId = CONTAINER_MANAGER.getContainerMappingNodeID(containerid);
//    nmClient.stopContainerAsync(containerid, nodeId);
//  }
//  private void askForContainer(String host, boolean relaxLocality, int askcontainerMemory) {
//    String[] hosts = {host};
//    int askcontainerCore = getContainerRequestVCores();
//    CONTAINER_MANAGER.getContainerInterval(askcontainerMemory).increaseAskForContainerNum();
//    askForContainerNumber++;
//    int priorityNum = askForContainerNumber;
//    //test
//
//    Resource containerResource = Records.newRecord(Resource.class);
//    containerResource.setMemory(askcontainerMemory);
//    containerResource.setVirtualCores(askcontainerCore);
//    Priority priority = Records.newRecord(Priority.class);
//    priority.setPriority(priorityNum);
//    ContainerRequest ask = new ContainerRequest(containerResource, hosts, null, priority, relaxLocality);
//
//    //throw request to RM
//    amRMClient.addContainerRequest(ask);
//
//    LOG.info("Asking for Container ,Memory :" + askcontainerMemory
//            + ",Host :" + host
//            + ",Core :" + askcontainerCore
//            + ",Priority :" + priority);
//  }
  public static void main(String[] args) {
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
    Date now = new Date();
    String strDate = sdfDate.format(now);

    ProcessTimer timer = ProcessTimer.getTimer();
    timer.settime();

    ApplicationMaster appMaster = null;
    String errorMessage = null;
    try {
      appMaster = new ApplicationMaster(args);

      ContainerGlobalVariable.RESOURCE_LOG_TYPE = AM_PREPARE;
      logServer = LogServerManagerImpl.getLogServerManagerImpl(drsConfiguration);
      logServer.createDblogTable();
      logServer.startResourceMonitor();
      appMaster.run();
      LOG.info("----- appMaster.run() success");
    } catch (ApplicationMasterException ex) {
      LOG.info("ApplicationMasterException:", ex);
      errorMessage = ex.getMessage();
    } catch (Exception ex) {
      LOG.info("main Exception:", ex);
      ex.printStackTrace();
      errorMessage = ex.toString();
    } finally {
      if (logServer != null) {
        LOG.info("PhaseLogCount:" + logServer.getPhaseLogCount());
        LOG.info("ResourceLogCount:" + logServer.getResourceLogCount());
        logServer.shutDown();
      }
      if (appMaster != null) {
        appMaster.close(errorMessage);
      }
    }

    long dif = timer.getDifferenceMillis();

    LOG.debug("Time different between SocketServer running to AM end :" + dif);
    now = new Date();
    strDate = sdfDate.format(now);
    LOG.debug("AM end main date:" + strDate);

    LOG.info("-----after appMaster try");
    EventQueue.isDispatchThread();

  }

  public void close(String errorMessage) {
    //amRMClient 是在run中才建立，要怎麼避免nullPointerException? logerver 現在要延到amRMClient後才能用
    ServiceStatus finalServiceStatus;

    if (socketServer != null) {
      socketServer.closeServerSucket();
    }

    if (socketServer == null) {
      finalServiceStatus = new ServiceStatus();
      finalServiceStatus.setStatus("Failed");
      finalServiceStatus.setErrorMessage("Failed before socketServer started.");
    } else if (socketServer.globals.getServiceStatus() == null) {
      finalServiceStatus = new ServiceStatus();
      finalServiceStatus.setStatus("Failed");
      finalServiceStatus.setErrorMessage("Failed before socketServer started.");
    } else {
      socketServer.globals.getServiceStatus().updateNodesStatus(schedule.getNodesStatus(), schedule.getProgress());
      socketServer.globals.getServiceStatus().setIsComplete(schedule.isComplete());

      if (socketServer.isServerKilled()) {
        socketServer.globals.getServiceStatus().setStatus("Killed by user.");
      } else if (final_status != null) {
        socketServer.globals.getServiceStatus().setStatus(final_status);
      } else if ((socketServer.globals.getServiceStatus().getProgress() == socketServer.globals.getServiceStatus().getFileCount())
              && socketServer.globals.isListThreadFinished()) {
        socketServer.globals.getServiceStatus().setStatus("Success FINISHED");
      } else {
        socketServer.globals.getServiceStatus().setStatus("Fail FINISHED");
        if (!errMsg.equals("")) {
          socketServer.globals.getServiceStatus().setErrorMessage(errMsg);
        }
      }
      if (socketServer.globals.getServiceStatus().getElapsed() == 0) {
        socketServer.globals.getServiceStatus().setElapsed(System.currentTimeMillis() - socketServer.globals.getServiceStatus().getStartTime());
      }
//            socketServer.serviceStatus.setEndTime(System.currentTimeMillis());
      finalServiceStatus = socketServer.globals.getServiceStatus();
    }

    if (errorMessage != null) {
      finalServiceStatus.setErrorMessage(errorMessage);
    }

    String finalResponse = null;
    try {
      LOG.info("finalServiceStatus.getStatus = " + finalServiceStatus.getStatus());
      finalResponse = objectToString(finalServiceStatus);
    } catch (IOException e) {
      LOG.debug(e);
    }
    if (finalResponse == null) {
      LOG.debug("finalResponse is null.");
    }
    try {
      if (schedule == null) {
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, finalResponse, null);
      } else if (schedule.isComplete() && schedule.getTaskAccNumber() != 0) {
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, finalResponse, null);
      } else if (schedule.getTaskAccNumber() == 0) {
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, finalResponse, null);
      } else {
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, finalResponse, null);
      }
    } catch (YarnException ex) {
      LOG.info("AM close YarnException,", ex);
    } catch (IOException ex) {
      LOG.info("AM close IOException,", ex);
    }

    nmClient.stop();
    amRMClient.stop();

    if (schedule != null) {
      schedule.closeListThread();
    }

    LOG.info("Closing nmClient/amRMClient...");
    nmClient.waitForServiceToStop(0);
    amRMClient.waitForServiceToStop(0);
    launchThreads.stream().forEach(Thread::interrupt);
    CONTAINER_MANAGER.showIntervalsLogChange();
    LOG.info("AM End.");

  }

  protected class ContainerLauncher implements Runnable {

    private Container container;
    private NMCallbackHandler containerListener;
    private String share_account;
    private String container_ID;

    public ContainerLauncher(Container container,
            NMCallbackHandler containerListener) {
      super();
      this.container = container;
      this.containerListener = containerListener;
      this.share_account = drsConfiguration.get(
              DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT);
      this.container_ID = container.getId().toString();
    }

    @Override
    public void run() {
      try {
        String command = this.getLaunchCommand(container);
        List<String> commands = new ArrayList<>();
        commands.add(command);
        LOG.info("Command to execute Container = " + command);

        //upload application master jar
        String appJar = drsConfiguration.get(DrsConfiguration.DISPATCH_YARN_JAR);
        LOG.info("Execute jar:" + appJar);
        String pathSuffix = DrsConfiguration.APP_NAME + "/" + APPID + "/" + appJar;
        Path filePath = new Path(share_account, pathSuffix);
//        LOG.info("===" + container_ID + "," + share_account + "," + pathSuffix);

        FileStatus destStatus = fs.getFileStatus(filePath);
        LocalResource jarResource = Records.newRecord(LocalResource.class);
        jarResource.setResource(ConverterUtils.getYarnUrlFromPath(filePath));
        jarResource.setSize(destStatus.getLen());
        jarResource.setTimestamp(destStatus.getModificationTime());
        jarResource.setType(LocalResourceType.FILE);
        jarResource.setVisibility(LocalResourceVisibility.PUBLIC);

        Map<String, LocalResource> localResources = distributeFile(APPID, fs);
        localResources.put(drsConfiguration.get("dispatch.yarn.jar"), jarResource);

        //wrap context of command
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setEnvironment(container_env);
        context.setLocalResources(localResources);
        context.setCommands(commands);
        //TODO check error
        containerListener.addContainer(container.getId(), container); //need to specify before start container 

        //start container
        nmClient.startContainerAsync(container, context);
        LOG.info("Container " + container.getId() + " launched!");

      } catch (IOException ex) {
        LOG.fatal(ex);
      }

    }

    /**
     * AM向啟動的Container 註冊HDFS上的SHARE FILE
     *
     * @param AppID
     * @param distrubute_fs
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private Map<String, LocalResource> distributeFile(String AppID, FileSystem distrubute_fs) {
      List<String> files = new ArrayList<>();
      Map<String, LocalResource> localResources = new HashMap<>();

      //jar
      files.add(CONFIG);
      files.add(R_CODE);
      if (DATA.endsWith("/")) {
        LOG.debug("Detected data is directory in AM");
      } else {
        files.add(DATA);
      }

      HdsClient hdsClient = new HdsClient(drsConfiguration);
      try {
        for (String dispatchResource : dispatchResourceList) {
          String fileName = FilenameUtils.getName(dispatchResource);
          String pathSuffix = DrsConfiguration.APP_NAME + "/" + APPID + "/" + fileName;
          Path dest = new Path(share_account, pathSuffix);

          FileStatus destStatus = distrubute_fs.getFileStatus(dest);
          LocalResource jarResource = Records.newRecord(LocalResource.class);
          jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
          jarResource.setSize(destStatus.getLen());
          jarResource.setTimestamp(destStatus.getModificationTime());
          jarResource.setType(LocalResourceType.FILE);
          jarResource.setVisibility(LocalResourceVisibility.PUBLIC);
          localResources.put(fileName, jarResource);
        }
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("Disppatch file not Success." + e.getLocalizedMessage());
      }

      //將dispatch source 從輸入路徑傳到hdfs指定路徑，再加入yarn lcoal resource中
      int count = 0;
      try {
        for (String file : files) {
          count++;
          //upload application master jar
          String pathSuffix = DrsConfiguration.APP_NAME + "/" + AppID + "/" + file;
          Path dest = new Path(share_account, pathSuffix);

          FileStatus destStatus = distrubute_fs.getFileStatus(dest);
          LocalResource jarResource = Records.newRecord(LocalResource.class);
          jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
          jarResource.setSize(destStatus.getLen());
          jarResource.setTimestamp(destStatus.getModificationTime());
          jarResource.setType(LocalResourceType.FILE);
          jarResource.setVisibility(LocalResourceVisibility.PUBLIC);
          localResources.put(file, jarResource);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return localResources;
    }

    public String getLaunchCommand(Container container) {

      double jvmHeapsizeRatio = drsConfiguration.getDouble(
              DrsConfiguration.DRS_JVM_HEAPSIZE_RATIO,
              DrsConfiguration.DRS_JVM_HEAPSIZE_RATIO_DEFAULT);
      if (jvmHeapsizeRatio < 0 || jvmHeapsizeRatio > 1) {
        LOG.warn("Value of \"drs.jvm.heapsize.ratio\"=" + jvmHeapsizeRatio + ",adjust to defult value.");
        jvmHeapsizeRatio = 0.8;
      }

      List<CharSequence> vargs = new ArrayList<>(500);

      vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
      //ava -Djava.library.path=/usr/lib64/R/library/rJava/jri -cp /usr/lib64/R/library/rJava/jri/JRI.jar:/opt/2016_dispatch/JR.jar jr.JR test.R

      int JVM_HeapSize = getJVMHeapSize(CONTAINER_MANAGER.getContainerResource(container.getId().toString()).getMemory(),
              jvmHeapsizeRatio);
      vargs.add("-Xmx" + JVM_HeapSize + "m");
//      vargs.add("-XX:+AlwaysPreTouch");
//      vargs.add("-XX:NativeMemoryTracking=summary");
      vargs.add("-Djava.library.path=" + drsConfiguration.get(DrsConfiguration.JRI_LIBRARY_PATH));
      vargs.add("com.dslab.drs.yarn.application.DRSContainer ");

//      LOG.info("413:JRI.library.path: {}", drsConfiguration.get("JRI.library.path"));
      try {
        vargs.add(Inet4Address.getLocalHost().getHostAddress());
        vargs.add(String.valueOf(socketServer.getPort()));
        vargs.add(container.getId().toString());
      } catch (UnknownHostException ex) {
        LOG.fatal(ex);
      }

      vargs.add(R_CODE);
      vargs.add(DATA);
      vargs.add(CONFIG);
      vargs.add(CODEOUT);
      vargs.add(COPYTO);

      vargs.add("1><LOG_DIR>/DRSContainer.stdout");
      vargs.add("2><LOG_DIR>/DRSContainer.stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }
      return command.toString();
    }
  }

  public int getJVMHeapSize(int containerSize, double ratio) {
//    LOG.info("getJVMHeapSize:" + containerSize + "," + ratio);
    double result = containerSize * ratio;
    return (int) result;
  }

//amrmcallbackhandler
  public class RMCallbackHandler implements CallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      LOG.info("###onContainersCompleted");

      for (ContainerStatus status : statuses) {
        ContainerId containerId = status.getContainerId();
        LOG.debug("Exit code = " + status.getExitStatus());
        String containerIdString = containerId.toString();
        if (!timeoutContainerID.contains(containerIdString)) {  //非time out
          LOG.info("Shutting down MyContainer in " + CONTAINER_MANAGER.getContainerMappingNodeID(containerId) + "(not timeout), Container: " + containerIdString);

          synchronized (runningContainerID) {
            runningContainerID.remove(containerIdString);
          }
          //重新分配正在執行任務
          if (strictPreemptionContainerIDs.contains(containerIdString) || preemptionContainerIDs.contains(containerIdString)) {
            LOG.info("Preemption container.");
            socketServer.clearContainerRunningTask(containerIdString, CONTAINER_MANAGER.getContainerMappingNodeID(containerId).toString().split(":")[0], true);
            schedule.getNodesStatus().get(containerIdString).setStatus("PREEMPTION");
            preemptionTimes.incrementAndGet();
          } else if (status.getExitStatus() == -104) {
            LOG.info(containerIdString + " OUT OF MEMORY,file = "
                    + SocketGlobalVariables.getAMGlobalVariable().getContaineridTaskMap().get(containerIdString));
            String resized = drsConfiguration.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE,
                    DrsConfiguration.DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE_DEFAULT);
            if (Boolean.parseBoolean(resized)) {
              if (!CONTAINER_MANAGER.isMaxInterval(containerIdString)) {
                OOMTimes.incrementAndGet();
              }
              for (String taskName : SocketGlobalVariables.getAMGlobalVariable().getContaineridTaskMap().get(containerIdString)) {
                long taskSize = SocketGlobalVariables.getAMGlobalVariable().getFileSize(containerIdString, taskName);
                CONTAINER_MANAGER.outOfMemoryUpdate(containerIdString, (int) taskSize);
              }
            }
            schedule.getNodesStatus().get(containerIdString).setStatus("OUT OF MEMORY");
          } else {
            socketServer.clearContainerRunningTask(containerIdString, CONTAINER_MANAGER.getContainerMappingNodeID(containerId).toString().split(":")[0], false);
            schedule.getNodesStatus().get(containerIdString).setStatus("FINISHED");
          }
        } else {
          schedule.getNodesStatus().get(containerIdString).setStatus("TIMEOUT");
          LOG.info(containerId + " TIMEOUT");
        }

        //紀錄結束時間
        schedule.getNodesStatus().get(containerIdString).colseContainer();

        LOG.info("Container completed : " + containerId);
        LOG.info("numCompletedContainers,now : " + numCompletedContainers.get()
                + ",ContainerId:" + containerIdString);

        // 由於 numCompletedContainers.incrementAndGet();，因此會觸發recovery程式碼的第一層判斷 isRunningContainerUnderExpected()
        // 但如果 if (!schedule.isComplete()) = true時，不會進入第二層判斷recoveryUnexpectContainer();
        // 即 not Timeout發生 且 所有運算中的任務完成時，不會觸發recovery，
        // AMExcpectContainer 永遠 > 0 不會結束DRS。
        // 更正方式，container非預期結束時，如果所有任務都算完，不對這個container做recovery，即decreaseAMExpectedContainer()。
        // PS: decreaseAMExpectedContainer() 後有會使AM提早結束，因此應該在此動作應該在Contianer確實關閉後才做前做好訊息的紀錄。
        if (schedule.isComplete()
                || SocketGlobalVariables.getAMGlobalVariable().isReleasedContainerID(containerIdString)) { //提早release的話也不觸發revocery
          SocketGlobalVariables.getAMGlobalVariable().decreaseAMExpectedContainer();
          CONTAINER_MANAGER.getContainerInterval(
                  CONTAINER_MANAGER.getContainerResource(containerIdString).getMemory())
                  .decreaseEventualContainerExpectedNum();
          LOG.info(containerIdString + " ,no recovery(decrease AM ExpectedContainer,current "
                  + SocketGlobalVariables.getAMGlobalVariable().getAMExpectedContainer() + ").");
        } else {
          LOG.info(containerIdString + " ,will recovery.");
        }

        //這行要注意，下了後會被另一個thread判斷說少一個而recovery
        CONTAINER_MANAGER.getContainerInterval(
                CONTAINER_MANAGER.getContainerResource(containerIdString).getMemory())
                .increaseCompletedContainerNum();
        numCompletedContainers.incrementAndGet();

      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Container  allocated,	 count = " + containers.size());
      for (Container container : containers) {
        String localNodeID = container.getNodeId().toString();
        String localContainerID = container.getId().toString();
        CONTAINER_MANAGER.putContainerResource(localContainerID, container.getResource());
        int localcontainerMemory = container.getResource().getMemory();
        int localCore = container.getResource().getVirtualCores();
        int Priority = container.getPriority().getPriority();
        LOG.info("Starting Container info = " + localNodeID
                + "," + localContainerID
                + "," + localcontainerMemory
                + "," + localCore
                + ",Priority=" + Priority);
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        LOG.info("NodeId :" + localNodeID + " ,containerId:" + localContainerID);

        ContainerLauncher launcher = new ContainerLauncher(container, containerListener);
        Thread thread = new Thread(launcher);
        thread.start();
        launchThreads.add(thread);

        CONTAINER_MANAGER.putContainerMappingNodeID(container.getId(), container.getNodeId());  //save map of containerId,NodeId for stop container purpose
        CONTAINER_MANAGER.putLaunchContainerStringID(container.getId().toString(), container.getId());

        socketServer.globals.logContainerTime(container.getId().toString(), System.currentTimeMillis());
        synchronized (runningContainerID) {
          runningContainerID.add(container.getId().toString());
        }

        schedule.getNodesStatus().put(localContainerID, new NodeStatus(localNodeID.split(":")[0], localContainerID));
        schedule.getNodesStatus().get(localContainerID).setMemorySize(localcontainerMemory);

        String interval = CONTAINER_MANAGER.getContainerInterval(localcontainerMemory).getBoundary();
        schedule.getNodesStatus().get(localContainerID).setContainerInterval(interval);
        schedule.getNodesStatus().keySet().stream()
                .filter(k -> schedule.getNodesStatus().get(k).isSameInterval(localcontainerMemory))
                .forEach(k -> schedule.getNodesStatus().get(k).setContainerInterval(interval));

      }
    }

    @Override
    public void onShutdownRequest() {
      LOG.info("###RMCallbackHandler onShutdownRequest ~container shut done! complete or fail ");
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
      LOG.info("###onNodesUpdated");
    }

    @Override
    public float getProgress() {
      float progress;
      if (schedule != null && schedule.getTaskNumber() != 0) {
        try {
          progress = (float) schedule.getProgress() / (float) schedule.getTaskNumber();
        } catch (Exception e) {
          progress = 0;
        }
      } else {
        progress = 0;
      }
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      LOG.info("###onError RMCallbackHandler.", e);
      if (e.getMessage().contains("Invalid resource request")) {
        errMsg = e.getMessage();
      }
      done = true;
      nmClient.stop();
      amRMClient.stop();
    }

    @Override
    public void onPreemption(PreemptionMessage preemptionMessage) {

      PreemptionContract preemptionContract = preemptionMessage.getContract();

      if (preemptionContract != null) {
        Set<PreemptionContainer> preemptionContainers = preemptionContract.getContainers();
        preemptionContainers.stream().forEach((preemptionContainer) -> {
          preemptionContainerIDs.add(preemptionContainer.getId().toString());
          LOG.info("preemptionContainer.getId:" + preemptionContainer.getId());
        });

        List<PreemptionResourceRequest> resourceRequest = preemptionContract.getResourceRequest();
        resourceRequest.stream().forEach((preemptionResourceRequest) -> {
          ResourceRequest request = preemptionResourceRequest.getResourceRequest();
          LOG.info("Containers:" + request.getNumContainers());
          LOG.info("Capability:" + request.getCapability().toString());
        });
      }
      StrictPreemptionContract strictPreemptionContract = preemptionMessage.getStrictContract();

      if (strictPreemptionContract != null) {
        Set<PreemptionContainer> strictPreemptionContainers = strictPreemptionContract.getContainers();
        strictPreemptionContainers.stream().forEach((preemptionContainer) -> {
          strictPreemptionContainerIDs.add(preemptionContainer.getId().toString());
          LOG.info("preemptionContainer.getId:" + preemptionContainer.getId());
        });
      }
    }
  }

}
