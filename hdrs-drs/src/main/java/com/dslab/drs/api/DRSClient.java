package com.dslab.drs.api;

import com.dslab.drs.exception.DrsArgumentsException;
import com.dslab.drs.exception.ConfigEmptyException;
import com.dslab.drs.exception.DrsJarUploadException;
import com.dslab.drs.exception.HdsListException;
import com.dslab.drs.yarn.application.ClientContext;
import com.dslab.drs.hdsclient.HdsClient;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.dslab.drs.exception.ApplicationIdNotFoundException;
import com.dslab.drs.exception.DrsClientException;
import com.dslab.drs.exception.KillApplicationException;
import com.dslab.drs.exception.SubmitApplicatioIOException;
import com.dslab.drs.exception.SubmitApplicationYarnException;
import com.dslab.drs.exception.UploadNecessaryFilesException;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import com.dslab.drs.container.ContainerTaskStatus;

import com.dslab.drs.exception.DrsHdsAuthException;
import com.dslab.drs.exception.DrsQueueException;
import com.dslab.drs.simulater.connection.hds.HdsRequester;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.zookeeper.QueueLock;
import com.dslab.drs.zookeeper.QueueLockImpl;
import java.net.MalformedURLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;

/**
 *
 * @author chen10
 */
public class DRSClient implements DRSservice, Closeable {

  private static final Log LOG = LogFactory.getLog(DRSClient.class);

  static final String SERVICE_NAME = "DRS";
  private String CLIENT_HOST_ADDRESS;

  private DrsConfiguration conf;
  private YarnClientApplication client;
  private YarnClient yarnClient;
  private ApplicationId appId;
  private int port = 0;
  private FileSystem fs;
  private ServiceStatus serviceStatus = new ServiceStatus();

  private boolean abortFromException = false;

  private String HDS_ACCESS_ADDRESS;
  private String DRS_CONFIG_LOCATION;
  private String YARN_RESOURCEMANAGER_ADDRESS;
  private ClientContext context;
  private Map<String, LocalResource> localResources = new HashMap<>();

  private String amhost;
  private boolean registered = false;
  private boolean watchRequestFlag = false;
  private boolean isClosed = false;
  private HdsClient hdsClient;

  private QueueLock lock;

  public DRSClient() {

  }

  @Override
  public void init(String code, String data, String config, String codeout, String copyto, String consoleto)
          throws DrsClientException, DrsArgumentsException {
    context = new ClientContext(code, data, config, codeout, copyto, consoleto);
    try {
      context.check();
    } catch (DrsArgumentsException ex) {
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + ex.getMessage());
      abortFromException = true;
      LOG.error("check arguments faild", ex);
      return;
    }
    initConf();
    YARN_RESOURCEMANAGER_ADDRESS = conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS);

    LOG.info("Get \"" + DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS + "\":" + YARN_RESOURCEMANAGER_ADDRESS);
    LOG.info("Get \"" + DrsConfiguration.APPLICATION_MASTER_MEMORY + "\":" + conf.getInt(DrsConfiguration.APPLICATION_MASTER_MEMORY, DrsConfiguration.APPLICATION_MASTER_MEMORY_DEFAULT));
    LOG.info("Get \"" + DrsConfiguration.R_HOME + "\":" + conf.get(DrsConfiguration.R_HOME));
    LOG.info("Get \"" + DrsConfiguration.LD_LIBRARY_PATH + "\":" + conf.get(DrsConfiguration.LD_LIBRARY_PATH));
    LOG.info("Get \"" + DrsConfiguration.DISPATCH_YARN_JAR + "\":" + conf.get(DrsConfiguration.DISPATCH_YARN_JAR));
    LOG.info("Get \"" + DrsConfiguration.JRI_LIBRARY_JAR_PATH + "\":" + conf.get(DrsConfiguration.JRI_LIBRARY_JAR_PATH));
    LOG.info("Get \"" + DrsConfiguration.DRS_HDS_AUTH_TOKEN + "\":" + conf.get(DrsConfiguration.DRS_HDS_AUTH_TOKEN, ""));

    if (YARN_RESOURCEMANAGER_ADDRESS == null) {
      String errorMessage = "Can't find yarn.resourcemanager.address from yarn-site.xml. ";
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + errorMessage);
      abortFromException = true;
      LOG.error("Can't find yarn.resourcemanager.address from yarn-site.xml. ");
      return;
    } else {
      YARN_RESOURCEMANAGER_ADDRESS = YARN_RESOURCEMANAGER_ADDRESS.split(":")[0];
      LOG.info("yarn_resourcemanager_address:" + YARN_RESOURCEMANAGER_ADDRESS);
    }

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    try {
      client = yarnClient.createApplication();
    } catch (IOException | YarnException ex) {
      String errorMessage = "Fail when create application :".concat(ex.getMessage());
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + errorMessage);
      abortFromException = true;
      LOG.error("DRSClientException:" + errorMessage, ex);
      return;
    }
    GetNewApplicationResponse appResponse = client.getNewApplicationResponse();
    appId = appResponse.getApplicationId();
    serviceStatus.setApplicationID(appId.toString());
    LOG.info("Applicatoin ID = " + appId.toString());

    try {
      hdsClient.checkHdsFileExist(conf, config);
      hdsClient.checkHdsFileExist(conf, code);
      if (data.endsWith("/")) {
        hdsClient.checkHdsDirectoryExist(conf, data);
      } else {
        hdsClient.checkHdsFileExist(conf, data);
      }

    } catch (HdsListException ex) {
      LOG.info("HdsListException:" + ex.getMessage());
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("HdsListException:" + ex.getMessage());
      abortFromException = true;
      LOG.error("HdsListException:" + ex.getMessage(), ex);
      return;
    } catch (DrsHdsAuthException ex) {
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DrsHdsAuthException: hds operation auth fail :" + ex.getMessage().replace("\"", ""));
      abortFromException = true;
      LOG.error("DrsHdsAuthException:" + ex.getMessage(), ex);
      return;
    }
    ArrayList<String> necessaryFiles = new ArrayList<>();
    if (data.endsWith("/")) {
      LOG.info("Detect data is directory No need to use hds list in DRSClient");
    } else {
      necessaryFiles.add(data);
    }

    necessaryFiles.add(config);
    necessaryFiles.add(code);

    try {
      localResources = uploadNecessaryFiles(localResources, necessaryFiles);
    } catch (Exception ex) {
      LOG.info("DRSClient UploadNecessaryFilesException :" + ex.getMessage());
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("UploadNecessaryFilesException:" + ex.getMessage());
      abortFromException = true;
      LOG.error("make sure serviceStatus.getErrorMessage :" + serviceStatus.getErrorMessage(), ex);
      return;
    }
    try {
      localResources = uploadDrsJar(localResources, "/opt/cloudera/parcels/CDH/lib/hbase/lib/" + conf.get(DrsConfiguration.DISPATCH_YARN_JAR));
    } catch (DrsJarUploadException ex) {
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DrsJarUploadException:" + ex.getMessage());
      abortFromException = true;
      LOG.error("DrsJarUploadException:" + ex.getMessage(), ex);
      return;
    }
  }

  public void initConf() throws DrsClientException {
    serviceStatus = new ServiceStatus();
    serviceStatus.setStatus("INITIAL");
    conf = DrsConfiguration.newDrsConfiguration();

    LOG.info("setURLStreamHandlerFactory:");

    try {
      if (fs == null) {
        fs = FileSystem.get(conf);
      }
    } catch (IOException ex) {
      String errorMessage = "getFileSystem error on DRSClient.init(), " + ex.getMessage();
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + errorMessage);
      abortFromException = true;
      LOG.error("DRSClientException:" + errorMessage, ex);
      return;
    }

    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    } catch (final Error e) {
      LOG.warn("Error in setURLStreamHandlerFactory: " + e.getMessage());
      e.printStackTrace();
    }

    try {
      if (DRS_CONFIG_LOCATION == null) {
        conf.addResource(new Path(System.getProperty("user.dir") + "/hds.xml"));
        DRS_CONFIG_LOCATION = conf.get("drs.config.location", "hdfs:///user/hbase/drs.xml");
        conf.addResource(new URL(DRS_CONFIG_LOCATION));
        HDS_ACCESS_ADDRESS = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS);

      } else {
        conf.addResource(new URL(DRS_CONFIG_LOCATION));
        HDS_ACCESS_ADDRESS = conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS);
      }
      if (HDS_ACCESS_ADDRESS == null || DRS_CONFIG_LOCATION == null) {
        throw new ConfigEmptyException("You didn't set \"hds.access.address\" or \"drs_config_location\"  in hbase-site.xml");
      }
    } catch (ConfigEmptyException | MalformedURLException ex) {
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("ConfigEmptyException:" + ex.getMessage());
      abortFromException = true;
      LOG.error("ConfigEmptyException:" + ex.getMessage(), ex);
      return;
    }

    LOG.info("initConf.");
    try {
      CLIENT_HOST_ADDRESS = Inet4Address.getLocalHost().getHostAddress();
      LOG.info("LocalHost:" + CLIENT_HOST_ADDRESS);
    } catch (Exception ex) {
      throw new DrsClientException("DRSClient fail to get host address.");
    }

    HDS_ACCESS_ADDRESS = HDS_ACCESS_ADDRESS.replaceAll("HOST-ADDRESS", CLIENT_HOST_ADDRESS);

    LOG.info("hds_access_address:" + HDS_ACCESS_ADDRESS);
    LOG.info("drs_config_location:" + DRS_CONFIG_LOCATION);

    LOG.info("dispatch.HDS.access.address :" + conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS));

    try {
      conf.addResource(new URL(DRS_CONFIG_LOCATION));
    } catch (IOException ex) {
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + "Can't Load conf from " + DRS_CONFIG_LOCATION + "," + ex.getMessage());
      abortFromException = true;
      LOG.error("DRSClientException:" + "Can't Load conf from " + DRS_CONFIG_LOCATION + "," + ex.getMessage(), ex);
      return;
    }
    LOG.info("dispatch.HDS.access.address :" + conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS));

    DrsConfiguration.replaceHdsConfHost(conf, CLIENT_HOST_ADDRESS);
//    conf = replaceConfHost(conf, CLIENT_HOST_ADDRESS);

    LOG.info("addResource:" + conf.get(DrsConfiguration.DRS_RM_CONFIG_LOCATION).concat("/yarn-site.xml"));
    conf.addResource(new Path(conf.get(DrsConfiguration.DRS_RM_CONFIG_LOCATION).concat("/yarn-site.xml")));
    LOG.info(DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS + ":" + conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS));

    hdsClient = new HdsClient(HDS_ACCESS_ADDRESS);
    hdsClient.setConf(conf);

    LOG.info("HdsClient(hds_access_address)");

    String RM_HA_ENABLED = conf.get("yarn.resourcemanager.ha.enabled");
    String RM_HA_RM_IDS = conf.get("yarn.resourcemanager.ha.rm-ids");

    LOG.info("RM_HA_ENABLED:" + RM_HA_ENABLED);
    LOG.info("RM_HA_RM_IDS:" + RM_HA_RM_IDS);

    if ("true".equals(RM_HA_ENABLED)) {
      LOG.info("Schedule for HA mode...");
      if (RM_HA_RM_IDS != null) {
        String[] rm_ids = RM_HA_RM_IDS.split(",");

        int connectionCount = 0;
        for (int i = 0; i < rm_ids.length; i++) {
          String rm_id = rm_ids[i];
          String tempHttpAddressPosrt = conf.get("yarn.resourcemanager.webapp.address." + rm_id);
          try {
            String rmHttpNodesPath = "http://" + tempHttpAddressPosrt + "/ws/v1/cluster/nodes";
            LOG.info("rmHttpNodesPath:" + rmHttpNodesPath);

            URL urlUse = new URL(rmHttpNodesPath);
            HttpURLConnection conn = (HttpURLConnection) urlUse
                    .openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    conn.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
            }
            in.close();

            LOG.info("yarn.resourcemanager.address.rm_id:" + conf.get("yarn.resourcemanager.address." + rm_id));

            break;
          } catch (Exception ex) {
            connectionCount++;
            LOG.info("The rm id connect RM fail:" + rm_id + "," + connectionCount + "/" + rm_ids.length + "," + " times");
            LOG.info(ex.getMessage());
            if (connectionCount == rm_ids.length) {
              LOG.info("=Can't connect to any host of yarn.resourcemanager.ha.rm-ids");
              throw new DrsClientException("Can't connect to any host of yarn.resourcemanager.ha.rm-ids");
            }
          }
        }

      } else {
        throw new DrsClientException("The value of yarn.resourcemanager.ha.rm-ids : null");
      }
    }

    LOG.info(DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS + ":" + conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS));

  }

  public void setKillEnviroment() throws KillApplicationException, DrsClientException {
    LOG.info("setKillEnviroment!");

    initConf();

    LOG.info("hds_access_address:" + HDS_ACCESS_ADDRESS);
    LOG.info("drs_config_location:" + DRS_CONFIG_LOCATION);

    hdsClient = new HdsClient(HDS_ACCESS_ADDRESS);
    conf = DrsConfiguration.newDrsConfiguration();

    LOG.info("addResource From HDFS:" + DRS_CONFIG_LOCATION);
    try {
      conf.addResource(new URL(DRS_CONFIG_LOCATION));
    } catch (IOException ex) {
      abortFromException = true;
      LOG.error("DRSClientException:" + "Can't Load conf from " + DRS_CONFIG_LOCATION + "," + ex.getMessage(), ex);
      return;
    }

    LOG.info("addResource:" + conf.get(DrsConfiguration.DRS_RM_CONFIG_LOCATION).concat("/yarn-site.xml"));
    conf.addResource(new Path(conf.get(DrsConfiguration.DRS_RM_CONFIG_LOCATION).concat("/yarn-site.xml")));

    YARN_RESOURCEMANAGER_ADDRESS = conf.get(DrsConfiguration.YARN_RESOURCEMANAGER_ADDRESS);

    if (YARN_RESOURCEMANAGER_ADDRESS == null) {
      String errorMessage = "Can't find \"yarn.resourcemanager.address\" from yarn-site.xml. ";
      LOG.info("Can't find \"yarn.resourcemanager.address\" from yarn-site.xml. ");
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + errorMessage);
      abortFromException = true;
      LOG.error("DRSClientException:" + errorMessage);
      return;
    } else {
      YARN_RESOURCEMANAGER_ADDRESS = YARN_RESOURCEMANAGER_ADDRESS.split(":")[0];
      LOG.info("yarn_resourcemanager_address =" + YARN_RESOURCEMANAGER_ADDRESS);
    }

    try {
      if (fs == null) {
        fs = FileSystem.get(conf);
      }
    } catch (IOException ex) {
      String errorMessage = "getFileSystem error on DRSClient.init(), " + ex.getMessage();
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("DRSClientException:" + errorMessage);
      abortFromException = true;
      LOG.error("DRSClientException:" + errorMessage, ex);
      return;
    }

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    LOG.info("setKillEnviroment done!");
  }

  public ApplicationId asyncStart() throws DrsClientException {
    if (abortFromException) {
      return null;
    }
    loadClientXml(conf);

    Long start_time = System.currentTimeMillis();

    // Create a new ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = client.getApplicationSubmissionContext();
    appContext.getKeepContainersAcrossApplicationAttempts();
    appContext.setApplicationName(SERVICE_NAME);
    appContext.setApplicationType(SERVICE_NAME);
    appContext.setApplicationId(appId);
    appContext.setMaxAppAttempts(conf.getInt(
            DrsConfiguration.APPLICATION_MAX_ATTEMPTS,
            DrsConfiguration.APPLICATION_MAX_ATTEMPTS_DEFAULT));

    //prepare am container (Env、Classpath)
    appContext.setAMContainerSpec(getAMContainerLaunchContext());

    //Request memory for AM Container
    appContext.setResource(getAMResource());

    //wait for get a queue name to run.
    String queueName = waitUntilCanRunOnQueue();
    appContext.setQueue(queueName);

    serviceStatus.setStatus("SUBMIT");

    try {
      submitApplication(yarnClient, appContext);
    } catch (SubmitApplicationYarnException | SubmitApplicatioIOException ex) {
      String errorMessage = ex.getMessage();
      serviceStatus.setStatus("Failed");
      serviceStatus.setErrorMessage("SubmitApplicationYarnException" + errorMessage);
      abortFromException = true;
      LOG.info("SubmitApplicationYarnException:" + errorMessage, ex);
    }

    try {
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      if (report != null) {
        amhost = report.getHost();
      }
    } catch (Exception ex) {   //why catch this?
      LOG.info("Get amhost occur Exception. " + ex.getMessage(), ex);
    }
    waitApplicationStart(yarnClient);
    LOG.info(appId + " - AM Started.");
    return appId;
  }

  private ContainerLaunchContext getAMContainerLaunchContext() {
    //prepare am container
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    amContainer.setLocalResources(localResources);
    amContainer.setEnvironment(getApplicationMasterEnv());
    amContainer.setCommands(getApplicationMasterCommands());
    return amContainer;
  }

  private Map<String, String> getApplicationMasterEnv() {
    Map<String, String> env = new HashMap<>();
    env.put("SERVICE_NAME", SERVICE_NAME);
    env.put("APPID", "" + appId.toString());
    env.put("APPNAME", "" + appId.toString());
    env.put("AMJAR", conf.get(DrsConfiguration.DISPATCH_YARN_JAR));
    env.put(DrsConfiguration.R_HOME, conf.get(DrsConfiguration.R_HOME));

    //Linux shell '\' 和 '$' 都是特殊字元，因此原本 '$' 需轉成 '\$'
    //Java RE中 '\','$'都是特殊字元， 將 "\" 轉成 "\\"，下面java函式才會有六個
    //ex : "a\\$b" 在第一次轉換時，會得到 "a$b"  ,dollar sing 被跳脫字元保留，不會視後面的字串為變數
    //     在第二次轉換時，會得到 "a" ，$b被當作變數。
    //避免$會吃掉變數，需保留 \\
    //Data如果是一個資料夾路徑，傳此路徑名稱，否則只傳檔名
    env.put("R_CODE", FilenameUtils.getName(context.getCode()).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
    if (context.getData().endsWith("/")) { //if is directory then 
      env.put("DATA", context.getData().replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
      LOG.info("DATA in DRSClient :" + context.getData());   //將 '\$' 取代為 '\\\$'
    } else {
      env.put("DATA", FilenameUtils.getName(context.getData()).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
      LOG.info("DATA in DRSClient not directory :" + FilenameUtils.getName(context.getData()));
    }
    if (context.getConsoleto() != null) {
      env.put("CONSOLEOUT", context.getConsoleto().replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
    }
    env.put("CONFIG", FilenameUtils.getName(context.getConfig()).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
    env.put("CODEOUT", context.getCodeout());
    env.put("COPYTO", context.getCopyto().replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));

    env.put("HDS_ACCESS_ADDRESS", HDS_ACCESS_ADDRESS);
    env.put("DRS_CONFIG_LOCATION", DRS_CONFIG_LOCATION);
    env.put("CLIENT_HOST_ADDRESS", CLIENT_HOST_ADDRESS);
    env.put("YARN_RESOURCEMANAGER_ADDRESS", YARN_RESOURCEMANAGER_ADDRESS);

    LOG.info("System's LD_LIBRARY_PATH " + System.getenv().get(DrsConfiguration.LD_LIBRARY_PATH));
    if (conf.get(DrsConfiguration.LD_LIBRARY_PATH) != null) {
      env.put(DrsConfiguration.LD_LIBRARY_PATH,
              System.getenv().get(DrsConfiguration.LD_LIBRARY_PATH) + ":" + conf.get(DrsConfiguration.LD_LIBRARY_PATH));
    } else {
      env.put(DrsConfiguration.LD_LIBRARY_PATH, "");
    }

    //set jar class path
    String classPath = getApplicationClassPath();

    env.put("CLASSPATH", classPath);
    return env;
  }

  private String getApplicationClassPath() {
    StringBuilder classPathEnv = new StringBuilder().append(File.pathSeparatorChar).append("./" + conf.get(DrsConfiguration.DISPATCH_YARN_JAR));
    classPathEnv.append(File.pathSeparatorChar).append(conf.get(DrsConfiguration.JRI_LIBRARY_JAR_PATH));
    classPathEnv.append(File.pathSeparatorChar).append("/opt/cloudera/parcels/CDH/lib/hbase/lib/*");
    for (String c
            : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(File.pathSeparatorChar);
    classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$());
    LOG.info("classPath:" + classPathEnv.toString());
    return classPathEnv.toString();
  }

  private List<String> getApplicationMasterCommands() {
    Vector<CharSequence> vargs = new Vector<>(30);
    vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
    vargs.add("-Xmx" + conf.getInt(DrsConfiguration.APPLICATION_MASTER_MEMORY, DrsConfiguration.APPLICATION_MASTER_MEMORY_DEFAULT) + "m");
    vargs.add("com.dslab.drs.yarn.application.ApplicationMaster");
    vargs.add("1><LOG_DIR>/AM.stdout");
    vargs.add("2><LOG_DIR>/AM.stderr");
    StringBuilder command = new StringBuilder();
    vargs.stream().forEach((str) -> {
      command.append(str).append(" ");
    });
    List<String> commands = new ArrayList<>();
    commands.add(command.toString());
    LOG.info("Command to execute ApplicationMaster =" + command);
    return commands;
  }

  private Resource getAMResource() {
    Resource amResource = Records.newRecord(Resource.class);
    amResource.setMemory(conf.getInt(
            DrsConfiguration.APPLICATION_MASTER_MEMORY,
            DrsConfiguration.APPLICATION_MASTER_MEMORY_DEFAULT));
    return amResource;
  }

  private String waitUntilCanRunOnQueue() throws DrsClientException {
    String queueName = null;
    String queueList = conf.get(DrsConfiguration.DRS_FAIR_QUEUE_LIST);
    if (queueList == null) {
      throw new DrsClientException("Please set \"" + DrsConfiguration.DRS_FAIR_QUEUE_LIST + "\".");
    }

    String[] queues = queueList.split(DrsConfiguration.DRS_FAIR_QUEUE_LIST_SPLITE_TOKEN);
    int queueWaitSize = conf.getInt(DrsConfiguration.DRS_FAIR_QUEUE_WAIT_SIZE,
            DrsConfiguration.DRS_FAIR_QUEUE_WAIT_SIZE_DEFAULT);

    try {
      if (conf.get(DrsConfiguration.ZOOKEEPER_QUORUM) == null) {
        throw new DrsClientException("Please set \"" + DrsConfiguration.ZOOKEEPER_QUORUM + "\".");
      }
      lock = new QueueLockImpl(conf, appId.toString(), queues, queueWaitSize);
      queueName = lock.waitUntilRunningOnQueues();
    } catch (IOException ex) {
      Logger.getLogger(DRSClient.class.getName()).log(Level.SEVERE, null, ex);
      LOG.info(appId + ", Get exception when lock :", ex);
      throw new DrsClientException(ex.getMessage());
    }
    if (queueName == null) {
      throw new DrsClientException("This job encounter unexpected error when waiting for queue.");
    }

    LOG.info("Get lock. do something for queue:" + queueName);
    return queueName;
  }

  private void loadClientXml(DrsConfiguration conf) {
    try {
      LOG.info("Update client conf...");
      String tempXmlPath = "hdfs:///tmp/DRS_" + System.getenv("HOSTNAME") + "_" + Thread.currentThread().getId() + "_" + System.currentTimeMillis() + "_temp.xml";
      LOG.info("temp client conf path : " + tempXmlPath);
      HdsRequester hdsRequester = new HdsRequester();
      hdsRequester.access(context.getConfig(), tempXmlPath, conf);
      conf.addResource(new URL(tempXmlPath));
      hdsRequester.delete(tempXmlPath, conf);
    } catch (IOException ex) {
      LOG.error("Update conf error", ex);
      //should add abortFromExcption??
    }
  }

  @Override
  public ApplicationId start() throws DrsClientException {
    return asyncStart();
  }

  private void waitApplicationStart(YarnClient yarnClient) throws DrsClientException {
    if (abortFromException) {
      LOG.error(appId + " - Aborted, skip wait application start");
      return;
    }
    int count = 0;

    while (count <= 60) {   //最多等60秒直到 job status 轉為 RUNNING && amhost not n/a
      try {
        LOG.info(appId + " - Wait Application Status to Running." + (++count));
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        amhost = report.getHost();

        YarnApplicationState state = report.getYarnApplicationState();

        if (!"ACCEPTED".equals(state.toString()) && !"INITIAL".equals(state.toString()) && !"SUBMIT".equals(state.toString()) && !"RUNNING".equals(state.toString())) {
          LOG.info(appId + " - State: " + state.toString() + ",AmHost: " + amhost);
          break;
        }

        if ("RUNNING".equals(state.toString()) && !"N/A".equals(amhost)) {
          break;
        }

      } catch (Exception ex) {
        throw new DrsClientException(appId + " failed when waitApplicationStart.");
      }
      try {
        Thread.sleep(1000);
      } catch (Exception ex) {
      }
    }
    count = 0;
    while (true) {
      try {
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        amhost = report.getHost();
        YarnApplicationState state = report.getYarnApplicationState();

        LOG.info(appId + " - State: " + state.toString() + ",AmHost: " + amhost);
        if (!"ACCEPTED".equals(state.toString()) && !"INITIAL".equals(state.toString()) && !"SUBMIT".equals(state.toString()) && !"RUNNING".equals(state.toString())) {
          break;
        }
      } catch (YarnException | IOException ex) {
        throw new DrsClientException("Failed when waitApplicationStart. ");
      }

      if (port == 0) {
        try {
          String pathSuffix = SERVICE_NAME + "/" + appId.toString() + "/port";
//          LOG.info("### appID = " + appId.toString());
          Path portPath = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
          FSDataInputStream fsStream = fs.open(portPath);
          String line = fsStream.readUTF();
          fsStream.close();
          port = Integer.parseInt(line);
          LOG.info(appId + " - port:" + port);
        } catch (IOException e) {
          LOG.info(appId + " - Wait socket port info");
        }

        try {
          String pathSuffix = SERVICE_NAME + "/" + appId.toString();
          Path folderPath = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
          if (!fs.exists(folderPath)) {
            LOG.info(appId + " - Folder not exist:" + SERVICE_NAME + "/" + appId.toString());
            break;
          }

        } catch (IOException e) {
          LOG.info(appId + " - Folder not exist:" + SERVICE_NAME + "/" + appId.toString());
        }
      } else {
        LOG.info(appId + " - Port :" + port);
        break;
      }
      try {
        Thread.sleep(3000);
      } catch (Exception ex) {
      }

    } //end while
    updateServiceStatus();
  }

  private void submitApplication(YarnClient yarnClient, ApplicationSubmissionContext appContext) throws SubmitApplicationYarnException, SubmitApplicatioIOException {
    try {
      yarnClient.submitApplication(appContext);
    } catch (YarnException ex) {
      throw new SubmitApplicationYarnException("Error when submit application:" + ex.getMessage());
    } catch (IOException ex) {
      throw new SubmitApplicatioIOException("Error when submit application:" + ex.getMessage());
    }
  }

  public void updateServiceStatus() {
    //serviceStatus hdfs_share_account yarnClient
    if (abortFromException) {
      LOG.error("Aborted, skip wait application start");
      return;
    }
    if (appId == null) {
      LOG.info("appId = null");
      return;
    }
//        serviceStatus.setStartTime(startTime);
    try {
//      LOG.info(appId + " - Update status.");
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      amhost = report.getHost();
      appId = report.getApplicationId();
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      LOG.debug(appId + " - state = " + state.toString());
      String response = null;
      if (state.toString().equals("ACCEPTED")) {
        serviceStatus.setStatus("ACCEPTED");
        LOG.info(appId + " - state:ACCEPTED");
      } else if (state.toString().equals("RUNNING")) {
        serviceStatus.setStatus("RUNNING");

        if (port == 0) {
          try {
            String pathSuffix = SERVICE_NAME + "/" + appId.toString() + "/port";
            Path portPath = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
            FSDataInputStream fsStream = fs.open(portPath);
            String line = fsStream.readUTF();
            fsStream.close();
            port = Integer.parseInt(line);
            LOG.info(appId + " - port:" + port);
          } catch (Exception e) {
            LOG.info(appId + " - Wait socket port info");
          }
        }

        LOG.debug(appId + " - Port:" + port);
        if (port != 0 && !amhost.equals("N/A")) {
          String amAddress = amhost.substring(amhost.indexOf("/") + 1, amhost.length());
          Socket appclient_socket = null;
          try {
            appclient_socket = new Socket(amAddress, port);//建立連線。(ip為伺服器端的ip，port為伺服器端開啟的port)
          } catch (IOException e) {
            LOG.error(appId + " - create socket failed: " + e.toString());
            LOG.error(appId + " - AM ADDRESS : " + amAddress + ",PORT = " + port);
            appclient_socket = null;
          }
          ContainerTaskStatus containerTaskInfo = new ContainerTaskStatus();
          if (appclient_socket != null) {

            ObjectOutputStream out = new ObjectOutputStream(appclient_socket.getOutputStream());
            BufferedReader reader = new BufferedReader(new InputStreamReader(appclient_socket.getInputStream()));

            if (watchRequestFlag) {
              LOG.info(appId + " - WatchRequest:" + report.getApplicationId().toString());
              containerTaskInfo.setCommand("WatchRequest");
              out.writeObject(containerTaskInfo);
              out.flush();
            } else if (!registered) {
              LOG.info(appId + " - AppClientRegister.");
              containerTaskInfo.setCommand("AppClientRegister");
              out.writeObject(containerTaskInfo);
              out.flush();
              registered = true;
            } else {
//              LOG.info(appId + " - AppClientAskProgress.");
              containerTaskInfo.setCommand("AppClientAskProgress");
              out.writeObject(containerTaskInfo);
              out.flush();
            }
            ObjectInputStream inFromServer = new ObjectInputStream(appclient_socket.getInputStream());
            this.serviceStatus = (ServiceStatus) inFromServer.readObject();
            appclient_socket.close();
          } else {
            LOG.info(appId + " - appclient_socket = null!!--------");
          }

        } else {
          LOG.info(appId + " - serviceStatus.setStatus(\"Initialize\")");
          LOG.info(appId + " - port:" + port + " amhost:" + amhost);
          serviceStatus.setStatus("Initialize");
          serviceStatus.setErrorMessage("Waiting AM open SocketServer to retrieve detail information");
        }
      } else if (YarnApplicationState.FINISHED == state) {
        LOG.info(appId + " - YarnApplicationState = FINISHED !!!");
        String diagnostics = report.getDiagnostics();
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          if (!diagnostics.equals("")) {
            String diagnosticsTmp = diagnostics.replaceAll("Attempt recovered after RM restart", "");
            serviceStatus = (ServiceStatus) stringToObject(diagnosticsTmp);
          }
          LOG.info(appId + " - FinalApplicationStatus = SUCCEEDED.");
        } else if (FinalApplicationStatus.FAILED == dsStatus) {
          if (!diagnostics.equals("")) {
            String diagnosticsTmp = diagnostics.replaceAll("Attempt recovered after RM restart", "");
            serviceStatus = (ServiceStatus) stringToObject(diagnosticsTmp);
          }
          LOG.info(appId + " - FinalApplicationStatus.FAILED ," + serviceStatus.getErrorMessage());
        } else {
          serviceStatus.setStatus("UNEXPECT_FINISH");
          LOG.info(appId + " - Application has finished unnormally.");
        }

      } else if (YarnApplicationState.KILLED == state) {
        LOG.info(appId + " - YarnApplicationState = kill");
        String diagnostics = report.getDiagnostics();
        serviceStatus.setStatus("KILLED");
        serviceStatus.setApplicationID(appId.toString());

        LOG.info(appId + " - Application has been killed.");
      } else if (YarnApplicationState.FAILED == state) {
        serviceStatus.setStatus("FAILED");
        LOG.info(appId + " - Application has been FAILED.");
      }

    } catch (Exception ex) {
      abortFromException = true;
      LOG.error(appId + " - updateServiceStatus error (not handle)", ex);
    }
  }

  public ServiceStatus watchServiceStatus(ApplicationId appId) {
    LOG.info("watchServiceStatus ");
    boolean watchRegistered = true;
    ServiceStatus watchServiceStatus = new ServiceStatus();
    int port = 0;
    try {
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      String amhost = report.getHost();
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      LOG.info(state.toString());
      String response = null;
      if (state.toString().equals("RUNNING")) {
        watchServiceStatus.setStatus("RUNNING");
        if (port == 0) {
          try {
            String pathSuffix = SERVICE_NAME + "/" + appId.toString() + "/port";
            Path portPath = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
//                        conf.addResource(new Path("/etc/hadoop/conf.cloudera.yarn/core-site.xml"));
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fsStream;
            fsStream = fs.open(portPath);
            String line = fsStream.readUTF();
            fsStream.close();
            port = Integer.parseInt(line);
            LOG.info("appId:" + appId + ",port:" + port);
          } catch (Exception e) {
            LOG.warn("Wait socket port info");
          }
        }
        LOG.debug("Port:" + port);
        if (port != 0 && !amhost.equals("N/A")) {
          String amAddress = amhost.substring(amhost.indexOf("/") + 1, amhost.length());
          Socket appclient_socket = null;
          try {
            appclient_socket = new Socket(amAddress, port);//建立連線。(ip為伺服器端的ip，port為伺服器端開啟的port)
          } catch (IOException e) {
            LOG.error("watchServiceStatus(socket): " + e.toString());
            appclient_socket = null;
          }
          ContainerTaskStatus containerTaskInfo = new ContainerTaskStatus();
          if (appclient_socket != null) {
//                        PrintStream writer = new PrintStream(appclient_socket.getOutputStream());
            ObjectOutputStream out = new ObjectOutputStream(appclient_socket.getOutputStream());
//                        BufferedReader reader = new BufferedReader(new InputStreamReader(appclient_socket.getInputStream()));
            if (!watchRegistered) {
              containerTaskInfo.setCommand("AppClientRegister");
              out.writeObject(containerTaskInfo);
              out.flush();
              watchRegistered = true;
            } else {
              containerTaskInfo.setCommand("AppClientAskProgress");
              out.writeObject(containerTaskInfo);
              out.flush();
            }
            ObjectInputStream infoFromServer = new ObjectInputStream(appclient_socket.getInputStream());
            watchServiceStatus = (ServiceStatus) infoFromServer.readObject();
            appclient_socket.close();
          } else {
            LOG.info("Server not running, appclient_socket = null.");
          }
        } else {
          watchServiceStatus.setStatus("Initialize");
          watchServiceStatus.setErrorMessage("Waiting AM open SocketServer to retrieve detail information");
          LOG.info("AM not running");
        }
        return watchServiceStatus;
      } else if (YarnApplicationState.FINISHED == state) {
        LOG.info("YarnApplicationState = FINISHED !!!");
        String diagnostics = report.getDiagnostics();
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          if (!diagnostics.equals("")) {
            watchServiceStatus = (ServiceStatus) stringToObject(report.getDiagnostics());
          }
          LOG.info("AM has completed successfully.");
        } else if (FinalApplicationStatus.FAILED == dsStatus) {
          if (!diagnostics.equals("")) {
            watchServiceStatus = (ServiceStatus) stringToObject(report.getDiagnostics());
          }
          LOG.info("AM has finished incorrectly,please check hds path correct.");
        } else {
          watchServiceStatus.setStatus("UNEXPECT_FINISH");
          LOG.info(" Application has finished unnormally.");
        }
      } else if (YarnApplicationState.KILLED == state) {
        LOG.info("YarnApplicationState = kill");
        String diagnostics = report.getDiagnostics();
        watchServiceStatus.setStatus("KILLED");
        watchServiceStatus.setApplicationID(appId.toString());

        LOG.info("Application has been killed.");
      } else if (YarnApplicationState.FAILED == state) {
        watchServiceStatus.setStatus("FAILED");
        LOG.info("Application has been FAILED.");
      }
    } catch (Exception ex) {
      LOG.error("watchServiceStatus: " + ex.getMessage());
    }
    return watchServiceStatus;
  }

  public ServiceStatus getServiceStatus() {
    LOG.info(appId + " - getServiceStatus...");
    if (serviceStatus == null) {
      serviceStatus = new ServiceStatus();
    }
    if (abortFromException) {
      return serviceStatus;
    }
    updateServiceStatus();
    return this.serviceStatus;
  }

  public void abort() {
    //更新狀態
    updateServiceStatus();

    LOG.info(appId + " - Try to kill on abort() .");
    if (appId != null) {
      try {
        yarnClient.killApplication(appId);
      } catch (Exception ex) {
        LOG.info(appId + " - killApplication fail");
        LOG.info(ex.getMessage(), ex);
      }
      yarnClient.stop();

    } else {
      LOG.info("Try to abort yarnClient which has not started or killed .");
    }
    LOG.info("abort end");
  }

  public boolean isAbort() {
    if (abortFromException) {
      LOG.info("In drsclient appID=" + appId + " isAbort abortFromException.");
      return true;
    }
    updateServiceStatus();
    // 2/3 號Michael回報一個錯誤，可能為Bug，任務都完成但是回傳訊息不是Success FINISHED  ，而是RUNNING
    // 有可能是作後回傳的status尚未更新至RM結束狀態

    //update initialize status
    if (serviceStatus.getStatus().equals("Initialize") || serviceStatus.getStatus().equals("INITIAL") || serviceStatus.getStatus().equals("SUBMIT") || serviceStatus.getStatus().equals("ACCEPTED") || serviceStatus.getStatus().equals("RUNNING")) {
      LOG.debug("In drsclient appID=" + appId + " isAbort prepare to abort state prepare to return false :" + serviceStatus.getStatus());
      return false;
    }
    LOG.debug("In drsclient appID=" + appId + " isAbort prepare to abort state :" + serviceStatus.getStatus());
    return true;
  }

  public void close() {
    LOG.info(appId + " - close DRS.");
    if (!isAbort()) {
      abort();
    }
    updateServiceStatus();

    if (lock != null) {
      try {
        lock.unlockQueue();
      } catch (KeeperException | InterruptedException | DrsQueueException ex) {
        Logger.getLogger(DRSClient.class.getName()).log(Level.SEVERE, null, ex);
        LOG.warn("unlockQueue.", ex);
      }
      lock.closeConnection();
      LOG.info(appId + " - Queue connection closed.");
    }

    if (isStopBeforeSubmit(appId)) {
      return;
    }
    //remove HDFS shareFolder

    String pathSuffix = SERVICE_NAME + "/" + appId.toString();
    try {
      Path dest = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
      fs.delete(dest, true);
      LOG.info(appId + " - delete:" + conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT) + "/" + pathSuffix);
    } catch (IOException ex) {
      LOG.warn(appId + " - delete error :" + conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT) + "/" + pathSuffix, ex);
    }

    LOG.info(appId + " - JSON:" + serviceStatus.toJSON());

    this.isClosed = true;
  }

  public String sendKillMessage(ApplicationId appId) throws KillApplicationException, IOException, YarnException {
    //serviceStatus hdfs_share_account yarnClient
    LOG.info("sendKillMessage();");
    String result = "unexpected error";
    if (abortFromException) {
      throw new KillApplicationException("abortFromException");
    }

//    try {
    ApplicationReport report = yarnClient.getApplicationReport(appId);

    amhost = report.getHost();
    YarnApplicationState state = report.getYarnApplicationState();
    FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

    LOG.info("appId:" + appId);
    LOG.info("amhost:" + amhost);
    LOG.info("getYarnApplicationState" + state.toString());

    String response = null;
    if (YarnApplicationState.KILLED == state) {
      result = "ApplcaitionID :" + appId + " already killed";
      return result;
    } else if (YarnApplicationState.FAILED == state) {
      result = "ApplcaitionID :" + appId + " status is FAILED can not be killed";
      return result;
    } else if (YarnApplicationState.FINISHED == state) {
      result = "ApplcaitionID :" + appId + " status is FINISHED can not be killed";
      return result;
    } else if (state.toString().equals("ACCEPTED")) {
      serviceStatus.setStatus("ACCEPTED");
    } else if (state.toString().equals("RUNNING")) {
      serviceStatus.setStatus("RUNNING");
      if (port == 0) {
        try {
          String pathSuffix = SERVICE_NAME + "/" + appId.toString() + "/port";
          LOG.info("pathSuffix:" + pathSuffix);
          LOG.info(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT + ":" + conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT));

          Path portPath = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
          LOG.info("portPath:" + portPath.toString());

          FSDataInputStream fsStream = fs.open(portPath);
          String line = fsStream.readUTF();
          fsStream.close();
          port = Integer.parseInt(line);
        } catch (Exception ex) {
          yarnClient.killApplication(appId);
          LOG.warn("try to force kill application.", ex);
        }
      }
      LOG.debug("Port:" + port);
      if (port != 0 && !amhost.equals("N/A")) {
        String amAddress = amhost.substring(amhost.indexOf("/") + 1, amhost.length());
        LOG.info("amAddress:" + amAddress);
        Socket appclient_socket = null;
        try {
          appclient_socket = new Socket(amAddress, port);//建立連線。(ip為伺服器端的ip，port為伺服器端開啟的port)
        } catch (IOException e) {
          LOG.warn(e.toString());
          appclient_socket = null;

        }

        ContainerTaskStatus containerTaskInfo = new ContainerTaskStatus();
        if (appclient_socket != null) {
          try {
            ObjectOutputStream out = new ObjectOutputStream(appclient_socket.getOutputStream());
            containerTaskInfo.setCommand("DrsClientKill");
            LOG.info("DrsClientAskForSaveStatus...........");
            out.writeObject(containerTaskInfo);
            out.flush();
            ObjectInputStream inFromServer = new ObjectInputStream(appclient_socket.getInputStream());
            this.serviceStatus = (ServiceStatus) inFromServer.readObject();
            appclient_socket.close();
          } catch (IOException | ClassNotFoundException ex) {
            yarnClient.killApplication(appId);
            LOG.info("Can't connect to socketServer,force kill " + appId, ex);
            return "Can't connect to socketServer, soft kill, force kill this.";
          }

        } else {
          LOG.info("appclient_socket = null!!--------");
        }

      } else {
        LOG.info("port != 0 && !amhost.equals(\"N/A\")");
        LOG.info(port);
        LOG.info("AM not running");
      }
//        result = "ApplcaitionID :" + appId + " kill success";
//        result = "Wait :" + appId + " killed.";

      //最多等60秒
      int maxWaitKilledTime = 60;
      for (int i = 0; i < maxWaitKilledTime; i++) {
        try {
          report = yarnClient.getApplicationReport(appId);
          amhost = report.getHost();
          state = report.getYarnApplicationState();
          dsStatus = report.getFinalApplicationStatus();
          LOG.info(appId + " - State: " + state.toString() + ",AmHost: " + amhost);
          if ("ACCEPTED".equals(state.toString()) || "INITIAL".equals(state.toString()) || "SUBMIT".equals(state.toString()) || "RUNNING".equals(state.toString())) {
            LOG.info("Wait application be killed..." + i);
          } else {
            LOG.info("Application had killed.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ex) {
              LOG.warn(ex.getMessage());
            }
            result = "ApplcaitionID :" + appId + " kill success";
            return result;
//              break;
          }
        } catch (YarnException | IOException ex) {
          LOG.warn("Error when kill when get states," + ex.getMessage(), ex);
        }
        try {
          Thread.sleep(1000);
        } catch (Exception ex) {
        }
      }

    }

    // Accepted or softkill failed , try to force kill 
    LOG.warn("Wait socketServer soft kill timeout, force kill" + appId);
    yarnClient.killApplication(appId);
    result = "ApplcaitionID :" + appId + " kill success";
    return result;
  }

  public KilldrsResponseMessage kill(String applicationId) throws KillApplicationException, ApplicationIdNotFoundException {
    checkApplicationIDNull(applicationId);

    LOG.info(applicationId + ", start kill.");

    ApplicationId killedApplicationId = null;

    try {
      killedApplicationId = getApplicationIdFromYarnClient(applicationId, yarnClient);
    } catch (IOException ex) {
      throw new KillApplicationException("ApplicationID :" + applicationId + " not found in running applications");
    }

    String result = "";
    try {
      result = sendKillMessage(killedApplicationId);
    } catch (IOException | YarnException ex) {
      Logger.getLogger(DRSClient.class.getName()).log(Level.SEVERE, null, ex);
      result = "send Kill Message failed.";
      LOG.warn("send Kill Message failed.", ex);
    }

    LOG.info(applicationId + " killed.");
    KilldrsResponseMessage killResponse = new KilldrsResponseMessage(result);
    return killResponse;
  }

  public ServiceStatus watch(String applicationId) throws ApplicationIdNotFoundException, DrsClientException {
    checkApplicationIDNull(applicationId);
    initConf();
    setWatchRequestFlag();
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    this.appId = getApplicationIdFromYarnClient(applicationId, yarnClient);
    if (fs == null) {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException ex) {
        LOG.error("fileSystem initial failed");
        LOG.error("ex " + ex.getMessage(), ex);
      }
    }
    return getServiceStatus();
  }

  public List<ApplicationStatus> watchAll() throws ApplicationIdNotFoundException, DrsClientException, YarnException, IOException {
    //watch client dont need to register
    LOG.info("In watchALL");
    initConf();
//    setRegistered(true);
    setWatchRequestFlag();
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    LOG.info("In watchALL -----");
    checkFsAndServiceStatusNull();
    if (fs == null) {
      LOG.info("fs = null");
      try {
        fs = FileSystem.get(conf);
      } catch (IOException ex) {
        LOG.error("fileSystem initial failed", ex);
      }
    }
    List<ApplicationStatus> applicationStatusList = new ArrayList();
    Map<ApplicationId, ApplicationReport> RunningApplicationMap = getRunningApplicationMap();
    List<ServiceStatus> ServiceStatusList = new ArrayList();

    for (ApplicationId AppID : RunningApplicationMap.keySet()) {
      LOG.info(AppID);
      ServiceStatus serviceStatus = watchServiceStatus(AppID);
      LOG.info("getNumUsedContainers :" + RunningApplicationMap.get(AppID).getApplicationResourceUsageReport().getNumUsedContainers());
      ServiceStatusList.add(serviceStatus);
      LOG.info("AppID dddd :" + AppID.toString());
      applicationStatusList.add(getApplicationStatus(serviceStatus, RunningApplicationMap.get(AppID)));
    }
    return applicationStatusList;
  }

  public List<ApplicationStatus> watchHistory(int limit) throws ApplicationIdNotFoundException, DrsClientException, YarnException, IOException {

    LOG.info("In watchHistory limit=" + limit);
    initConf();
    setWatchRequestFlag();

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    checkFsAndServiceStatusNull();
    if (fs == null) {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException ex) {
        LOG.info("fileSystem initial failed");
      }
    }
    List<ApplicationStatus> applicationStatusList = new ArrayList();
    Map<ApplicationId, ApplicationReport> HistoryApplicationMap = getDRSHistoryApplicationMap(limit);
    List<ServiceStatus> ServiceStatusList = new ArrayList();
    LOG.info("HistoryApplicationMap.size() :" + HistoryApplicationMap.size());

    ArrayList<ApplicationId> keys = new ArrayList<ApplicationId>(HistoryApplicationMap.keySet());
    LOG.info("Keys size :" + keys.size());

    for (int i = keys.size() - 1; i >= keys.size(); i--) { //reverse
      LOG.info(HistoryApplicationMap.get(keys.get(i)).getApplicationId());
    }

    for (ApplicationId appId : HistoryApplicationMap.keySet()) {
      LOG.info(appId);
      ServiceStatus serviceStatus = watchServiceStatus(appId);
      ServiceStatusList.add(serviceStatus);

      applicationStatusList.add(getApplicationStatus(serviceStatus, HistoryApplicationMap.get(appId)));
    }
    return applicationStatusList;
  }

  private ApplicationStatus getApplicationStatus(ServiceStatus serviceStatus, ApplicationReport report) {
    ApplicationStatus application = new ApplicationStatus();
    try {
      if (serviceStatus.getApplicationID() == null) {
        application.setApplicationID(report.getApplicationId().toString());
      } else {
        application.setApplicationID(serviceStatus.getApplicationID());
      }
      if (serviceStatus.getAMnode().equals("")) {
        application.setAMnode(report.getHost());
      } else {
        application.setAMnode(serviceStatus.getAMnode());
      }
      application.setStatus(serviceStatus.getStatus());
      application.setCode(serviceStatus.getCode());
      application.setData(serviceStatus.getData());
      application.setConfig(serviceStatus.getConfig());
      application.setCopyto(serviceStatus.getCopyto());
      application.setElapsed(serviceStatus.getElapsed());
      application.setFileCount(serviceStatus.getFileCount());
      application.setProgress(serviceStatus.getProgress());
    } catch (Exception ex) {
      LOG.error("Get application status failed," + ex.getMessage(), ex);
    }
    application.setUsedContainers(report.getApplicationResourceUsageReport().getNumUsedContainers());
    application.setUsedVirtualCores(report.getApplicationResourceUsageReport().getUsedResources().getVirtualCores());
    application.setUsedVirtualMemory(report.getApplicationResourceUsageReport().getUsedResources().getMemory());

    return application;
  }

  private void checkFsAndServiceStatusNull() {
    if (fs == null) {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException ex) {
        LOG.warn("check FileSystem", ex);
      }
    }
    if (serviceStatus == null) {
      serviceStatus = new ServiceStatus();
    }
  }

  private Map<ApplicationId, ApplicationReport> getRunningApplicationMap() throws ApplicationIdNotFoundException {

    Map<ApplicationId, ApplicationReport> ApplicationMap = new LinkedHashMap();
    List<ApplicationReport> applicationReports;
    Set DRSApplicationType = new HashSet();
    DRSApplicationType.add(SERVICE_NAME);
    try {
      applicationReports = yarnClient.getApplications(DRSApplicationType);
    } catch (YarnException | IOException ex) {
      throw new ApplicationIdNotFoundException("Can't get applicationReports from yarnClient.");
    }
    for (ApplicationReport applicationReport : applicationReports) {
      if (applicationReport.getYarnApplicationState().toString().equals("RUNNING") && applicationReport.getName().equals("DRS")) {
        ApplicationMap.put(applicationReport.getApplicationId(), applicationReport);
      }
    }
    return ApplicationMap;
  }

  private Map<ApplicationId, ApplicationReport> getDRSHistoryApplicationMap(int limit) throws ApplicationIdNotFoundException {
    if (limit < 0) {
      limit = 50000;
    }
    int count = 1;
    Map<ApplicationId, ApplicationReport> ApplicationMap = new LinkedHashMap();
    TreeMap<Long, ApplicationReport> ApplicationSortMap = new TreeMap(Collections.reverseOrder());
    List<ApplicationReport> applicationReports;
    Set DRSApplicationType = new HashSet();
    DRSApplicationType.add(SERVICE_NAME);
    try {
      applicationReports = yarnClient.getApplications(DRSApplicationType);
    } catch (YarnException | IOException ex) {
      throw new ApplicationIdNotFoundException("Can't get applicationReports from yarnClient.");
    }

    for (ApplicationReport applicationReport : applicationReports) {
      ApplicationSortMap.put(applicationReport.getStartTime(), applicationReport);
    }

    int times = 0;
    if ((ApplicationSortMap.size() - 1) - limit >= 0) {
      times = (ApplicationSortMap.size() - 1) - limit;
    }

    for (ApplicationReport applicationReport : ApplicationSortMap.values()) {
      if (applicationReport.getName().equals("DRS") && !applicationReport.getYarnApplicationState().toString().equals("RUNNING") && !applicationReport.getYarnApplicationState().toString().equals(YarnApplicationState.ACCEPTED) && !applicationReport.getYarnApplicationState().toString().equals(YarnApplicationState.SUBMITTED)) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        String strDate = sdfDate.format(applicationReport.getStartTime());
        if (count <= limit) {
          ApplicationMap.put(applicationReport.getApplicationId(), applicationReport);
        }
        count++;
      }
    }
    return ApplicationMap;
  }

  private ApplicationId getApplicationIdFromYarnClient(String applicationId, YarnClient yarnClient) throws ApplicationIdNotFoundException {
    List<ApplicationReport> applicationReports;
    try {
      applicationReports = yarnClient.getApplications();
    } catch (YarnException | IOException ex) {
      throw new ApplicationIdNotFoundException("Can't get applicationReports from yarnClient.");
    }
    for (ApplicationReport applicationReport : applicationReports) {
      if (applicationReport.getApplicationId().toString().equals(applicationId)) {
        return applicationReport.getApplicationId();
      }
    }
    throw new ApplicationIdNotFoundException("ApplicationID : " + applicationId + " not found ");
  }

  private boolean isStopBeforeSubmit(ApplicationId appId) {
    if (appId == null) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isClosed() {
    return isClosed;
  }

  private static Object stringToObject(String string) throws IOException, ClassNotFoundException {
    org.apache.commons.codec.binary.Base64 base64 = new org.apache.commons.codec.binary.Base64();
    byte[] data = base64.decode(string);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    Object o = ois.readObject();
    ois.close();
    return o;
  }

  private void getHdsFiles(List<String> temp_files) throws IOException {
    for (String file_path : temp_files) {
      String fileName = null;
      String[] temp = file_path.split("/");
      if (temp.length > 0) {
        fileName = "/tmp/temp_" + appId.toString() + "_" + temp[temp.length - 1];
      } else {
        LOG.warn("input empty FileName:" + file_path);
        continue;
      }
      File file = new File(fileName);
      if (file.exists()) {
        file.delete();
        LOG.info("delete " + fileName);
      }

      StringBuilder response = new StringBuilder();
      boolean getFile = true;
      String address = HDS_ACCESS_ADDRESS + "?from=";
      String query = address + file_path + "&to=local:///" + fileName;

//            ftp://$ftp-chia/yarm-conf.xml&to= local:///" + fileName;
//            ftp://chia7712:!@#$%@192.103.0.99/
      URL url = new URL(query);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();

      try (InputStream stream = con.getInputStream()) {
        Files.copy(stream, Paths.get(fileName));
      } catch (Exception e) {
        e.printStackTrace();
        getFile = false;
      }
      //get response
      StringBuilder builder = new StringBuilder();
      builder.append(con.getResponseCode())
              .append(" ")
              .append(con.getResponseMessage())
              .append("\n");

      Map<String, List<String>> map = con.getHeaderFields();
      for (Map.Entry<String, List<String>> entry : map.entrySet()) {
        if (entry.getKey() == null) {
          continue;
        }
        builder.append(entry.getKey())
                .append(": ");
        List<String> headerValues = entry.getValue();
        Iterator<String> it = headerValues.iterator();
        if (it.hasNext()) {
          builder.append(it.next());

          while (it.hasNext()) {
            builder.append(", ")
                    .append(it.next());
          }
        }
        builder.append("\n");
      }
      response = builder;
    }

  }

  public String prepareRequestUrl(String address, String from, String to) {
    String request = address.concat("?from=").concat(from).concat("&to=").concat(to);
    return request;
  }

  private Map<String, LocalResource> uploadDrsJar(Map<String, LocalResource> localResources, String path) throws DrsJarUploadException {
    try {
      Path src = new Path(path);
      String fileName = FilenameUtils.getName(path);
      String pathSuffix = SERVICE_NAME + "/" + appId.toString() + "/" + fileName;;
      Path dest = new Path(conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT), pathSuffix);
      dest = fs.makeQualified(dest);
      fs.copyFromLocalFile(false, true, src, dest);
      FileStatus destStatus = fs.getFileStatus(dest);
      LocalResource jarResource = Records.newRecord(LocalResource.class);
      jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
      jarResource.setSize(destStatus.getLen());
      jarResource.setTimestamp(destStatus.getModificationTime());
      jarResource.setType(LocalResourceType.FILE);
      jarResource.setVisibility(LocalResourceVisibility.PUBLIC);
      localResources.put(fileName, jarResource);

      return localResources;
    } catch (IOException ex) {
      throw new DrsJarUploadException("Can't up load drs jar," + ex.getMessage());
    } catch (Exception ex) {
      throw new DrsJarUploadException("Can't up load drs jar," + ex.getMessage());
    }
  }

  private Map<String, LocalResource> addDrsConfLocalRsource(Map<String, LocalResource> localResources, String path) throws UploadNecessaryFilesException {
    try {
      Path src = new Path(path);
      String fileName = FilenameUtils.getName(path);
      Path dest = new Path(path);

      FileStatus destStatus = fs.getFileStatus(dest);
      LocalResource jarResource = Records.newRecord(LocalResource.class
      );

      dest = fs.makeQualified(dest);
      jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
      jarResource.setSize(destStatus.getLen());
      jarResource.setTimestamp(destStatus.getModificationTime());
      jarResource.setType(LocalResourceType.FILE);
      jarResource.setVisibility(LocalResourceVisibility.PUBLIC);
      localResources.put(fileName, jarResource);

      return localResources;
    } catch (IOException ex) {
      throw new UploadNecessaryFilesException("add local resource erroe:" + FilenameUtils.getName(path) + "," + ex.getMessage());
    }
  }

  private Map<String, LocalResource> uploadNecessaryFiles(Map<String, LocalResource> localResources, ArrayList<String> paths) throws UploadNecessaryFilesException {

    ArrayList<String> failList = new ArrayList<String>();
    HashMap<String, String> faiReseaonMap = new HashMap<String, String>();

    for (String file_path : paths) {
      try {
        String fileName = FilenameUtils.getName(file_path);
        String pathSuffix = SERVICE_NAME + "/" + appId.toString() + "/" + fileName;

        String hdfs_share_account = conf.get(DrsConfiguration.DISPATCH_HDFS_SHARE_ACCOUNT);
        Path dest = new Path(hdfs_share_account, pathSuffix);
        if (!hdfs_share_account.startsWith("hdfs")) {
          hdfs_share_account = "hdfs://" + hdfs_share_account;
        }
        hdsClient.hdsAccessService(file_path, hdfs_share_account + "/" + pathSuffix);
        dest = fs.makeQualified(dest);
        LOG.info("dest uploadNecessaryFiles:" + dest);
        FileStatus destStatus = fs.getFileStatus(dest);
        LocalResource jarResource = Records.newRecord(LocalResource.class);
        jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
        LOG.info("uploadNecessaryFiles Url:" + ConverterUtils.getYarnUrlFromPath(dest));
        jarResource.setSize(destStatus.getLen());
        jarResource.setTimestamp(destStatus.getModificationTime());
        jarResource.setType(LocalResourceType.FILE);
        jarResource.setVisibility(LocalResourceVisibility.PUBLIC);
        localResources.put(fileName, jarResource);
      } catch (Exception ex) {
        LOG.info(appId + " uploadNecessaryFiles fail:" + FilenameUtils.getName(file_path));
        failList.add(FilenameUtils.getName(file_path));
        faiReseaonMap.put(FilenameUtils.getName(file_path), ex.getMessage().replace("\"", ""));
        LOG.info(ex.getMessage(), ex);
      }
    }

    if (failList.size() > 0) {
      StringBuilder failMessage = new StringBuilder();
      for (Map.Entry<String, String> entry : faiReseaonMap.entrySet()) {
        failMessage.append(entry.getKey() + " :");
        failMessage.append(entry.getValue() + ". ");
      }
      failMessage.deleteCharAt(failMessage.lastIndexOf(","));
      failMessage.append("fail.");

      LOG.info("at failList.size() >0 :" + failMessage.toString());
      throw new UploadNecessaryFilesException(failMessage.toString());
    }
    return localResources;
  }

  public void setDrsConfigLocation(String drs_config_location) {
    LOG.info("setDrsConfigLocation:" + drs_config_location);
    this.DRS_CONFIG_LOCATION = drs_config_location;
  }

  public void setHdsAccessAddress(String hds_access_address) {
    LOG.info("setHdsAccessAddress:" + hds_access_address);
    this.HDS_ACCESS_ADDRESS = hds_access_address;
  }

  public void setWatchRequestFlag() {
    this.watchRequestFlag = true;
  }

  private void checkApplicationIDNull(String applicationID) throws ApplicationIdNotFoundException {
    if ("".equals(applicationID)) {
      throw new ApplicationIdNotFoundException("ApplicationID not specified!");
    }
  }

}
