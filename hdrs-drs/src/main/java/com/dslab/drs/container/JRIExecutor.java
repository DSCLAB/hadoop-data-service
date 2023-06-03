package com.dslab.drs.container;

import com.dslab.drs.drslog.DrsPhaseLogHelper;
import com.dslab.drs.utils.ProcessTimer;
import com.dslab.drs.exception.JRIException;
import com.dslab.drs.exception.RrunTimeException;
import com.dslab.drs.yarn.application.ClientContext;
import com.dslab.drs.yarn.application.ExtractZip;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.rosuda.JRI.Rengine;
import org.apache.hadoop.net.NetUtils;
import org.rosuda.JRI.REXP;
import com.dslab.drs.hdsclient.HdsClient;
import static com.dslab.drs.monitor.MonitorConstant.R_COMPUTE;
import static com.dslab.drs.monitor.MonitorConstant.R_GET_FILES;
import static com.dslab.drs.monitor.MonitorConstant.R_PUT_RESULTS;
import com.dslab.drs.simulater.connection.hds.HdsRequester;
import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.yarn.application.ContainerGlobalVariable;
import com.dslab.drs.yarn.application.DRSContainer;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author caca
 */
public final class JRIExecutor extends Executor {

  private static final Log LOG = LogFactory.getLog(JRIExecutor.class);

  private final Rengine engine;
  private final RTextConsole rtextconsole;
  private ContainerTaskStatus taskStatus;
  private final YarnConfiguration conf;
  boolean ExecuteSuccess = true;
  private final String containerID;
  private final ClientContext context;
  private final String TaskUrl;
  private String TaskName;
  private String CurrentTarget;
  private String CurrentTargetPath;
  private String CurrentTargetType;
  private String NodeName;

  private String directory_path;
  private String ErrorLogPath;
  private String localTaskDir;
  private String Unzip_File_Path;
  private String Console_File_Path;

  private String Output_DirList_R_var;
  private String Output_Dir_R_var;
  private String Input_Path_R_var;

  private String CurrentTaskTargetPath;
  private String ErrorMessage = "none";
  private String Source_R_script;
  private final static String SLASH = "/";

  String[] DRS_output_list;
  String[] DRS_copyto_list;
  Exception ex;
  ProcessTimer timer = ProcessTimer.getTimer();

  public JRIExecutor(Rengine engine, RTextConsole rtextconsole, YarnConfiguration conf,
          String TaskUrl, String container_id, ClientContext context) {
    this.engine = engine;
    this.rtextconsole = rtextconsole;
    this.conf = conf;
    this.TaskUrl = TaskUrl;
    this.containerID = container_id;
    this.context = context;
  }

  @Override
  boolean initial() {
    LOG.debug("Init JriExecutor.");
    directory_path = "/tmp/".concat(containerID).concat(SLASH);
    Source_R_script = context.getCode();
    ErrorLogPath = directory_path.concat(conf.get(DrsConfiguration.DISPATCH_NODE_SCRIPT_LOG, DrsConfiguration.DISPATCH_NODE_SCRIPT_LOG_DEFAULT)) + SLASH;
    localTaskDir = directory_path.concat(conf.get(DrsConfiguration.DISPATCH_NODE_FILES, DrsConfiguration.DISPATCH_NODE_FILES_DEFAULT)) + SLASH;
    Unzip_File_Path = directory_path.concat(conf.get(DrsConfiguration.DISPATCH_NODE_CONTENTS, DrsConfiguration.DISPATCH_NODE_CONTENTS_DEFAULT)) + SLASH;
    Console_File_Path = directory_path.concat(conf.get(DrsConfiguration.DISPATCH_NODE_CONSOLE, DrsConfiguration.DISPATCH_NODE_CONSOLE_DEFAULT)) + SLASH;
    Output_DirList_R_var = conf.get(DrsConfiguration.DISPATCH_SCRIPT_OUTPUT_PATH_LIST, DrsConfiguration.DISPATCH_SCRIPT_OUTPUT_PATH_LIST_DEFAULT);
    Output_Dir_R_var = conf.get(DrsConfiguration.DISPATCH_SCRIPT_OUTPUT_PATH, DrsConfiguration.DISPATCH_SCRIPT_OUTPUT_PATH_DEFAULT);
    Input_Path_R_var = conf.get(DrsConfiguration.DISPATCH_SCRIPT_INPUT_PATH, DrsConfiguration.DISPATCH_SCRIPT_INPUT_PATH_DEFAULT);

    DRS_output_list = context.getCodeout().split(DrsConfiguration.DRS_HDS_SPLITE_TOKEN);

    for (int i = 0; i < DRS_output_list.length; i++) {
      DRS_output_list[i] = (new File(conf.get(DrsConfiguration.DISPATCH_NODE_OUTPUT, DrsConfiguration.DISPATCH_NODE_NODE_DEFAULT), DRS_output_list[i])).getPath();
      DRS_output_list[i] = (new File(directory_path, DRS_output_list[i])).getPath();
      DRS_output_list[i] = DRS_output_list[i] + SLASH;
    }
    DRS_copyto_list = context.getCopyto().split(DrsConfiguration.DRS_HDS_SPLITE_TOKEN);

    TaskName = FilenameUtils.getName(TaskUrl);
    CurrentTarget = TaskName;
    CurrentTaskTargetPath = localTaskDir.concat(TaskName);
    NodeName = NetUtils.getHostname();
    taskStatus = new ContainerTaskStatus(TaskUrl, TaskName, "R", NodeName);
    List<String> ZipContents = new ArrayList<>();
    ErrorMessage = checkEngine();

    if (!checkError(ErrorMessage)) {
      ExecuteSuccess = false;
      ex = new JRIException("JRI initial stage fail during checking R engine version");
      LOG.debug(ex.getMessage());
      taskStatus.setErrorMessage(ex);
    }

    if (!ExecuteSuccess && ex == null) {
      ex = new JRIException("JRI initial stage fail");
    }
    return ExecuteSuccess;
  }

  @Override
  boolean prepareFiles() {
//    System.out.println("prepareFiles stage-------------");

    HdsClient hdsget = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT));
    hdsget.setConf(conf);
    try {
      ExecuteSuccess = hdsget.getClientData(TaskUrl, localTaskDir);
    } catch (Exception e) {
      ExecuteSuccess = false;
      ex = new JRIException("JRI prepareFiles stage fail during download download target file :".concat(e.getMessage()));
      LOG.debug("In prepareFiles encounter error");
    }

    CurrentTargetType = FilenameUtils.getExtension(CurrentTaskTargetPath);
    if (ExecuteSuccess) {
      if ("true".equals(conf.get(DrsConfiguration.DISPATCH_NODE_ZIP_EXTRACT_ENABLE, DrsConfiguration.DISPATCH_NODE_ZIP_EXTRACT_ENABLE_DEFAULT))) {
        //if (CurrentTargetType.equals("zip") && conf.get(DRSConstants.DISPATCH_NODE_ZIP_EXTRACT_ENABLE).equals("true")) {
        try {
          List<String> ZipContents = ExtractZip.unzip(CurrentTaskTargetPath, Unzip_File_Path);
          CurrentTarget = ZipContents.get(0); //if zip only content 1 file
          CurrentTargetPath = Unzip_File_Path.concat(CurrentTarget);

        } catch (Exception e) {
          LOG.debug("JRI_Client ExtractZip encounter error");
          ex = new JRIException("JRI prepareFiles stage fail during extract zip files :".concat(e.getMessage()));
          ExecuteSuccess = false;
          LOG.debug(e.getMessage());
        }
      } else {
        LOG.debug("Input data type not zip");
        CurrentTarget = TaskName;
        CurrentTargetPath = CurrentTaskTargetPath;
      }
    }

    if (rtextconsole != null && ExecuteSuccess) {
      LOG.debug(Console_File_Path.concat(DrsConfiguration.R_CONSOLE_PREFIX.concat(FilenameUtils.removeExtension(CurrentTarget).concat(".txt"))));
      rtextconsole.setConsolePath(Console_File_Path.concat(DrsConfiguration.R_CONSOLE_PREFIX.concat(FilenameUtils.removeExtension(CurrentTarget).concat(".txt"))));
    }
    if (ExecuteSuccess) {
      ExecuteSuccess = clearREnvironment(conf, engine);
    }

//    System.out.println("prepareFiles stage-------------end");
    if (!ExecuteSuccess && ex == null) {
      ex = new JRIException("JRI prepareFiles stage fail");
    }
    long timeDiff = timer.getDifferenceMillis();
    DRSContainer.logStore.addPhaseLog(DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(), ContainerGlobalVariable.APPLICATION_NAME, containerID, TaskUrl, R_GET_FILES, timeDiff));
    return ExecuteSuccess;
  }

  @Override
  boolean main() {

    try {
      engine.assign("containerID", containerID);
      engine.assign(Input_Path_R_var, CurrentTargetPath);
      engine.assign(Output_DirList_R_var, DRS_output_list);
      engine.assign(Output_Dir_R_var, DRS_output_list[0]);
    } catch (Exception e) {
      LOG.error("R variable assign encounter error.");
      ExecuteSuccess = false;
      e.printStackTrace();
    }
    //parse and assign user specified variables
    Map UserREnv = parseUserEnvironment(conf);
    boolean REnvAssign = assignREnvironment(conf, engine, UserREnv);
    //beforeCode execute
    DrsUdfManager udfmanager = new DrsUdfManager(conf, engine);
    udfmanager.init();
    if (udfmanager.hasUdfBeforeCode()) {
      ExecuteSuccess = udfmanager.executeBeforeCode(engine);
      if (!ExecuteSuccess) {
        ex = udfmanager.getException(engine);
      }
    }

    if (REnvAssign && ExecuteSuccess) {
      Calendar c1 = Calendar.getInstance();

      //R main execution
      if (sourceRscript(engine, Source_R_script)) {
        LOG.info("Run Script Successed.");
      } else {
        LOG.info("Run Script Failed.");
        ExecuteSuccess = false;
        LOG.debug("JRI_Client R runtime encounter error");
        String errMessage = getRerrMessage(engine);

        ex = new RrunTimeException("JRI R run time fail --".concat(errMessage));
        try {
          recordRError(errMessage, ErrorLogPath, CurrentTarget);
        } catch (IOException ex) {
          LOG.error("record R error fail");
          LOG.error(ex.toString());
        }
      }
    } else {
      if (ex == null) {
        ex = new JRIException("JRI main stage fail during Assigning R Code execute fail");
      }
      ExecuteSuccess = false;
    }
    //afterCode execute
    if (ExecuteSuccess && udfmanager.hasUdfAfterCode()) {
      ExecuteSuccess = udfmanager.executeAfterCode(engine);
      if (!ExecuteSuccess) {
        ex = udfmanager.getException(engine);
      }
    }

    if (!ExecuteSuccess && ex == null) {
      ex = new JRIException("JRI main stage fail");
    }
    // Difference in seconds
    long timeDiff = timer.getDifferenceMillis();
    DRSContainer.logStore.addPhaseLog(DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(),
            ContainerGlobalVariable.APPLICATION_NAME, containerID, TaskUrl, R_COMPUTE, timeDiff));
    taskStatus.setTaskRunTime(timeDiff);

    return ExecuteSuccess;
  }

  @Override
  boolean outputFiles() {
    HdsClient hdsput = new HdsClient(conf.get(DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS, DrsConfiguration.DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT));
    hdsput.setConf(conf);
    
    for (int i = 0; i < DRS_output_list.length; i++) {
      String localOutFolder = DRS_output_list[i];
      String copytoFolder = DRS_copyto_list[i];
      if (ExecuteSuccess) {
        //上傳codeout to copytoFolder
        HdsRequester hdsRequestor = new HdsRequester();
        try {
          List<String> successfulOutputFiles
                  = hdsRequestor.uploadFolderFilesToHds(localOutFolder, copytoFolder, new DrsConfiguration(conf));
          taskStatus.addOutputFiles(successfulOutputFiles);
        } catch (Exception e) {
          e.printStackTrace();
          ex = new JRIException("JRI outputFiles stage fail during uploading (copyto):" + e.getMessage());
          ExecuteSuccess = false;
        } finally {
          deleteDirectoryFiles(localOutFolder);
        }
      }
    }

    if ((context.getConsoleto() != null) && (!context.getConsoleto().equals(""))) {
      try {
        //上傳Console to consoleto
        HdsRequester hdsRequestor = new HdsRequester();
        String consoleFileName = FilenameUtils.getName(Console_File_Path);
        String remoteConsole = hdsRequestor.mergePath(context.getConsoleto(), consoleFileName);
        List<String> successfulOutputFiles
                = hdsRequestor.uploadFolderFilesToHds(Console_File_Path, remoteConsole, new DrsConfiguration(conf));
        taskStatus.addOutputFiles(successfulOutputFiles);
      } catch (Exception e) {
        e.printStackTrace();
        ex = new JRIException("JRI outputFiles stage fail during uploading (console):" + e.getMessage());
        ExecuteSuccess = false;
      }
      deleteDirectoryFiles(Console_File_Path);
    }

    long timeDiff = timer.getDifferenceMillis();
    DRSContainer.logStore.addPhaseLog(DrsPhaseLogHelper.getDrsPhaseLog(timer.getLastTime(),
            ContainerGlobalVariable.APPLICATION_NAME, containerID, TaskUrl, R_PUT_RESULTS, timeDiff));
    return ExecuteSuccess;
  }

  @Override
  ContainerTaskStatus getResult() {
    taskStatus.setResult(ExecuteSuccess);
    if (ex != null) {
      taskStatus.setErrorMessage(ex);
    }
    return taskStatus;
  }

  public String checkEngine() {
    String Check_JRI_Engine = "none error during JRI engine check";
    try {
      System.loadLibrary("jri");
    } catch (UnsatisfiedLinkError e) {
      ExecuteSuccess = false;
      String errStr = "all environment variables (PATH, LD_LIBRARY_PATH, etc.) are setup properly (see supplied script)";
      String libName = "libjri.so";
      if (System.getProperty("os.name").startsWith("Window")) {
        Check_JRI_Engine = errStr = "you start JGR by double-clicking the JGR.exe program";
        libName = "jri.dll";
      }
      if (System.getProperty("os.name").startsWith("Mac")) {
        Check_JRI_Engine = errStr = "you start JGR by double-clicking the JGR application";
        libName = "libjri.jnilib";
      }
      System.err.println("JRI_Client Cannot find JRI native library!\n");
      e.printStackTrace();
      System.exit(1);
    }
    if (!Rengine.versionCheck()) {
      LOG.debug("JRI_Client Java/R Interface (JRI) library doesn't match this JGR version.\nPlease update JGR and JRI to the latest version.");
      Check_JRI_Engine = "JRI_Client Java/R Interface (JRI) library doesn't match this JGR version.\nPlease update JGR and JRI to the latest version";
      System.exit(2);
      ExecuteSuccess = false;
    }

    return Check_JRI_Engine;
  }

  public boolean checkError(String ErrorMessage) {
    return ErrorMessage.startsWith("none");
  }

  public boolean clearREnvironment(YarnConfiguration conf, Rengine engine) {
    boolean clearRenv = false;
    try {
      engine.eval("rm(list=ls())");
      engine.eval("gc()");

      clearRenv = true;
    } catch (Exception e) {
      LOG.error("clear R environment encounter error");
      e.printStackTrace();
    }
    return clearRenv;
  }

  public static Map parseUserEnvironment(YarnConfiguration conf) {
    Map UserREnv = new HashMap();
    String userEnv = conf.get(DrsConfiguration.DISPATCH_SCRIPT_ENVIRONMENT_VALUE, DrsConfiguration.DISPATCH_SCRIPT_ENVIRONMENT_VALUE_DEFAULT);
    try {
      for (String content : userEnv.split(";")) {
        String[] pairs = content.split(":");
        UserREnv.put(pairs[0], pairs[1]);
      }
    } catch (Exception e) {
      LOG.debug("Parse user enviroment encounter error");
      e.printStackTrace();
      UserREnv = null;
    }

    return UserREnv;
  }

  public static boolean assignREnvironment(YarnConfiguration conf, Rengine engine, Map UserREnv) {
    boolean AssignSuccess = false;
    try {
      for (HashMap.Entry<String, String> entry : (Set<Map.Entry>) UserREnv.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        engine.assign(key, value);
      }

      AssignSuccess = true;
    } catch (Exception e) {
      LOG.error("Assinging User enviroment encounter error");
      e.printStackTrace();
      AssignSuccess = false;
    }
    return AssignSuccess;
  }

  public void recordRError(String content, String errorlogpath, String filename) throws IOException {
    String LogPath = errorlogpath.concat(filename);
    LOG.info("Record R Error to " + LogPath);
    LogPath = LogPath.concat("_error.txt");
    File newTextFile = new File(LogPath);
    try (FileWriter fileWriter = new FileWriter(newTextFile)) {
      fileWriter.write(content);
      fileWriter.close();
    }
  }

  public static boolean sourceRscript(Rengine engine, String script) {
    //如果source失敗   Rengin 不會建立 REXP 物件
    boolean sourceSuccess = true;
    engine.eval("options(show.error.locations=TRUE)");
    REXP sourceResult = engine.eval("nonCatchError <- source('" + script + "')");
    if (sourceResult == null) {
      sourceSuccess = false;
    }
    return sourceSuccess;
  }

  public static String getRerrMessage(Rengine engine) {
    REXP errmessage = engine.eval("errorResult <- geterrmessage()");
    return errmessage.asString();
  }

  private void deleteDirectoryFiles(String path) {
    File file = new File(path);
    if (!file.isDirectory()) {
      LOG.warn("Delete directory'sfiles fail," + path + " is not a directory");
      return;
    }
    File[] files = file.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
  }
}
