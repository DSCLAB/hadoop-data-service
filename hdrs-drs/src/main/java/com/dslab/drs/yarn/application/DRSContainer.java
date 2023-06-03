package com.dslab.drs.yarn.application;

/**
 *
 * @author kh87313
 */
import com.dslab.drs.hdsclient.HdsClient;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import com.dslab.drs.drslog.LogClientManagerImpl;
import com.dslab.drs.drslog.LogStore;
import com.dslab.drs.utils.ProcessTimer;
import com.dslab.drs.drslog.LogClientManager;
import static com.dslab.drs.monitor.MonitorConstant.CONTAINER_PREPARE;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

public final class DRSContainer {

  private static final Log LOG = LogFactory.getLog(DRSContainer.class);
  // DrsLogStore 會暫存 Phase、Resource 的 Log 訊息
  public static final LogStore logStore = LogStore.getLogStore();

  public static void main(String[] args) throws IOException {

    ProcessTimer timer = ProcessTimer.getTimer();
    timer.settime();
    String timeStamp = new SimpleDateFormat("yyyy/MM/dd__HH:mm:ss").format(Calendar.getInstance().getTime());
    LOG.info("Current time :" + timeStamp);
    LOG.info("Hostname :" + NetUtils.getHostname());
    LOG.info("Container started on " + NetUtils.getHostname());
    Date here = new Date();
    LOG.info("Container started time : " + here.getTime());

    String hostIP = args[0];

    int port = Integer.parseInt(args[1]);
    LOG.info("Socket port:" + port);

    String containerId = args[2];
    String code = args[3];
    String data = args[4];
    String config = args[5];
    String codeout = args[6];

    Map<String, String> env = System.getenv();
    String copyto = env.get("COPYTO");
    String consoleout = env.get("CONSOLEOUT");

    ContainerGlobalVariable.APPLICATION_ID = env.get("APPID");
    ContainerGlobalVariable.APPLICATION_NAME = env.get("APPNAME");
    ContainerGlobalVariable.CONTAINER_ID = containerId;

    LOG.info("containerID:" + containerId);
    LOG.info("code:" + code);
    LOG.info("data:" + data);
    LOG.info("config:" + config);
    LOG.info("codeout:" + codeout);
    ///TODO
    LOG.info("copyto:" + copyto);
    LOG.info("consoleout:" + consoleout);

    String HDS_ACCESS_ADDRESS = env.get("HDS_ACCESS_ADDRESS");
    String DRS_CONFIG_LOCATION = env.get("DRS_CONFIG_LOCATION");
    String CLIENT_HOST_ADDRESS = env.get("CLIENT_HOST_ADDRESS");

    LOG.info("env variable HDS_ACCESS_ADDRESS:" + HDS_ACCESS_ADDRESS);
    LOG.info("env variable DRS_CONFIG_LOCATION:" + DRS_CONFIG_LOCATION);
    LOG.info("env variable CLIENT_HOST_ADDRESS:" + CLIENT_HOST_ADDRESS);

    HdsClient hdsClient = new HdsClient(HDS_ACCESS_ADDRESS);
    DrsConfiguration conf = DrsConfiguration.newDrsConfiguration();

    conf.addResource(FilenameUtils.getName(DRS_CONFIG_LOCATION));
    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    } catch (final Error e) {
      LOG.info("Error in setURLStreamHandlerFactory: catch and continue" + e.getMessage());
    }
    try {
      conf.addResource(new URL(DRS_CONFIG_LOCATION));
    } catch (MalformedURLException ex) {
      LOG.fatal(ex);
    }
    conf.addResource(new Path(config));

    DrsConfiguration.setDrsDefaultValue(conf);
    DrsConfiguration.replaceHdsConfHost(conf, CLIENT_HOST_ADDRESS);

    LOG.info("Log level : " + conf.getLogLever().toString());
    Logger.getLogger("com.dslab.drs").setLevel(conf.getLogLever());

    String dispatchResourceConf = conf.get("dispatch.resource");
    LOG.info("Get \"dispatch.resource\":" + dispatchResourceConf);
    String nodeName = NetUtils.getHostname().split("/")[0];

    ClientContext context = new ClientContext(code, data, config, codeout, copyto, consoleout);
    initial(conf, containerId, context);

    SocketClient client = new SocketClient(hostIP, port, nodeName, containerId, conf, context);

    //LogClient會定期將資訊Push 到SocketServer
    LogClientManager drsClientLogManager = LogClientManagerImpl.getLogClientManagerImpl(conf, hostIP, port);
    ContainerGlobalVariable.RESOURCE_LOG_TYPE = CONTAINER_PREPARE;
    drsClientLogManager.startLogClientSocket();
    drsClientLogManager.startResourceMonitor();
    drsClientLogManager.startClientResizeMonitor();
    drsClientLogManager.setContainerID(containerId);
    client.add(drsClientLogManager);
    client.run();
    drsClientLogManager.shutDown();
    LOG.info("Container is end.");
  }

  static boolean initial(YarnConfiguration conf, String container_ID, ClientContext context) {
    try {
      File dispatchDirectory = new File("/tmp/".concat(container_ID));
      if (!dispatchDirectory.exists()) {
        dispatchDirectory.mkdir();
      }
      createDispatchDirectory(conf, container_ID, context);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static void createDispatchDirectory(YarnConfiguration conf, String container_id, ClientContext context) {

    String logsFolder = conf.get(DrsConfiguration.DISPATCH_NODE_SCRIPT_LOG);
    String filesFolder = conf.get(DrsConfiguration.DISPATCH_NODE_FILES);
    String contentsFolder = conf.get(DrsConfiguration.DISPATCH_NODE_CONTENTS);
    String outputFolder = conf.get(DrsConfiguration.DISPATCH_NODE_OUTPUT);
    String consoleFolder = conf.get(DrsConfiguration.DISPATCH_NODE_CONSOLE);

    File directory_path = new File("/tmp/", container_id);

    File ErrorLogPath = new File(directory_path, logsFolder);
    File HDS_File_Path = new File(directory_path, filesFolder);
    File Unzip_File_Path = new File(directory_path, contentsFolder);
    File Console_File_Path = new File(directory_path, consoleFolder);
    File Output_Path = new File(directory_path, outputFolder);

    ArrayList<File> directories = new ArrayList<>();
    directories.add(ErrorLogPath);
    directories.add(HDS_File_Path);
    directories.add(Unzip_File_Path);
    directories.add(Output_Path);
    directories.add(Console_File_Path);

    String[] codeoutList = context.getCodeout().split(DrsConfiguration.DRS_HDS_SPLITE_TOKEN);
    for (String codeout : codeoutList) {
      directories.add((new File(Output_Path, codeout)));
    }
    try {
      for (File file : directories) {
        LOG.info("Create R local folder:" + file.getPath());
        if (!file.exists()) {
          file.mkdirs();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
