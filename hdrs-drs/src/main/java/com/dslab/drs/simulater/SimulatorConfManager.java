package com.dslab.drs.simulater;

import static com.dslab.drs.simulater.Workload.*;
import com.dslab.drs.simulater.connection.hds.HdsRequester;
import com.dslab.drs.simulater.generator.NumberListGenerator;
import com.dslab.drs.utils.DrsConfiguration;
import static com.dslab.drs.utils.DrsConfiguration.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import static com.dslab.drs.simulater.Workload.IS_DRS_ASYNC;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;

/**
 *
 * @author Weli,kh87313
 */
public final class SimulatorConfManager {

  private static final Log LOG = LogFactory.getLog(SimulatorConfManager.class);
  private final DrsConfiguration simulaterConf;
  private final Workload workload;

  private final NumberListGenerator intputListGenerator;

  public SimulatorConfManager(String xml, String text) throws IOException {
    this.simulaterConf = DrsConfiguration.newDrsConfiguration();
    this.simulaterConf.addResource(new Path(xml));
    this.workload = new Workload(text);
    DrsConfiguration.replaceHdsConfHost(simulaterConf, workload.getStringValue(Workload.HDS_HOST));
    this.intputListGenerator = new NumberListGenerator(getMinFleNumber(), getMaxFleNumber());
  }

  public DrsConfiguration getSimulaterConf() throws IOException {
    return simulaterConf;
  }

  public Configuration getDbConf() {
    return workload.getDbConf();
  }

  public Workload getSimulaterWorkload() throws IOException {
    return workload;
  }

  //生成一個屬於各Thread專屬的xml檔
  public DrsConfiguration generatorThreadConf() {
    DrsConfiguration threadConf = DrsConfiguration.newDrsConfiguration();
    threadConf.addResource(simulaterConf);
    packageThreadConf(threadConf);
    return threadConf;
  }

  private void packageThreadConf(DrsConfiguration threadConf) {
    //TODO

    //設定記憶體
    //設定檔案數量
    //
  }

  public DrsArguments prepareDrsFiles(String threadName) throws IOException {
    Path inputFolder = new Path(workload.getStringValue(Workload.LOCAL_TEMP_FOLDER), Workload.INPUTFILE_FOLDER);
    Path xmlFolder = new Path(workload.getStringValue(Workload.LOCAL_TEMP_FOLDER), Workload.XML_FOLDER);
    //因為hdfs:/// 有三條斜線，不能使用Path("","") remote.temp.folder
    new File(inputFolder.toString()).mkdirs();
    new File(xmlFolder.toString()).mkdirs();

    String remoteTempFolder = getRemoteTempFolder();
    String fileExtension = getFileExtension();

    String remoteResourceFolder = remoteTempFolder + Workload.RESOURCE_FOLDER;

    //建立本地檔案
    Path inputTxtPath = createNewInputTxt(inputFolder, threadName, remoteResourceFolder, fileExtension);
    Path confXmlPath = createNewConfXml(xmlFolder, threadName, generatorThreadConf());

    Path rScriptPath = new Path(workload.getStringValue(Workload.R_SCRIPT_PATH));

    String remoteInputPath = remoteTempFolder + Workload.INPUTFILE_FOLDER + inputTxtPath.getName();
    String remoteXmlPath = remoteTempFolder + Workload.XML_FOLDER + confXmlPath.getName();
    String remoteScriptPath = remoteTempFolder + Workload.R_SCRIPTE_FOLDER + threadName + "_" + rScriptPath.getName();

    HdsRequester hdsRequestor = new HdsRequester();
    hdsRequestor.uploadToHds(inputTxtPath.toString(), remoteInputPath, simulaterConf);
    hdsRequestor.uploadToHds(confXmlPath.toString(), remoteXmlPath, simulaterConf);
    hdsRequestor.uploadToHds(rScriptPath.toString(), remoteScriptPath, simulaterConf);

    String codeout = workload.getStringValue(Workload.CODEOUT);
    String copyto = workload.getStringValue(Workload.COPYTO) + threadName;
    String consoleto = workload.getStringValue(Workload.CONSOLETO);

    DrsArguments drsArgument = new DrsArguments(remoteScriptPath, remoteInputPath, remoteXmlPath, codeout, copyto, consoleto);
//    waitFileCanList(drsArgument, 20);
    return drsArgument;
  }

  private Path createNewInputTxt(Path inputFolder, String threadName, String remoteResourceFolder, String fileExtension) throws IOException {
    Path inputTxtPath = new Path(inputFolder, threadName + "_input.txt");
    String nl = System.getProperty("line.separator");
    List<Integer> intputTxtList = intputListGenerator.nextValue();

    LOG.info("set " + intputTxtList.size() + " input file for " + threadName);
    SimulatorShareObject.addTotalThreadInputFileNumber(threadName, intputTxtList.size());

    try (FileWriter fw = new FileWriter(inputTxtPath.toString()); BufferedWriter bw = new BufferedWriter(fw)) {
      for (Integer fileNumber : intputTxtList) {
        bw.write(remoteResourceFolder + fileNumber + "." + fileExtension + nl);
      }
    }
    return inputTxtPath;
  }

  public Path createNewConfXml(Path xmlFolder, String threadName, DrsConfiguration conf) throws IOException {
    Path confXmlPath = new Path(xmlFolder, threadName + ".xml");
    Document document = DocumentHelper.createDocument();
    Element element = document.addElement("configuration");

    putSavedValues(element, DISPATCH_HDS_LIST_ADDRESS, conf);
    putSavedValues(element, DISPATCH_HDS_RUN_ADDRESS, conf);
    putSavedValues(element, DISPATCH_HDS_KILL_ADDRESS, conf);
    putSavedValues(element, DISPATCH_HDS_WATCH_DRS_ADDRESS, conf);
    putSavedValues(element, DRS_SLAVES, conf);
    putSavedValues(element, DRS_HDS_AUTH_TOKEN, conf);
    putSavedValues(element, APPLICATION_MASTER_MEMORY, conf);
    putSavedValues(element, APPLICATION_LAUNCH_CONTAINER_NUMBER, conf);
    putSavedValues(element, APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN, conf);
    putSavedValues(element, CONTAINER_MEMORY_MB, conf);
    putSavedValues(element, CONTAINER_CPU_VCORES, conf);
    putSavedValues(element, DRS_JVM_HEAPSIZE_RATIO, conf);
    putSavedValues(element, CONTAINER_MAX_SUSPEND_TIME_SEC, conf);
    putSavedValues(element, CONTAINER_MAX_RETRY_TIMES, conf);
    putSavedValues(element, CONTAINER_TASK_RETRY_TIMES, conf);
    putSavedValues(element, DISPATCH_SCHEDULER, conf);
    putSavedValues(element, DISPATCH_LOCALITYSCHEDULER_THRESHOLD, conf);
    putSavedValues(element, DISPATCH_LOCALITYSCHEDULER_TIMEOUT, conf);
    putSavedValues(element, DISPATCH_LOCALITY_RATIO, conf);
    putSavedValues(element, DRS_UDF_DIR, conf);
    putSavedValues(element, DRS_UDF_EXECUTE_BEFORECODE, conf);
    putSavedValues(element, DRS_UDF_EXECUTE_AFTERCODE, conf);
    putSavedValues(element, DISPATCH_SCRIPT_INPUT_PATH, conf);
    putSavedValues(element, DISPATCH_SCRIPT_OUTPUT_PATH, conf);
    putSavedValues(element, DISPATCH_SCRIPT_OUTPUT_PATH_LIST, conf);
    putSavedValues(element, DISPATCH_SCRIPT_ENVIRONMENT_VALUE, conf);
    putSavedValues(element, DISPATCH_NODE_SCRIPT_LOG, conf);
    putSavedValues(element, DISPATCH_NODE_FILES, conf);
    putSavedValues(element, DISPATCH_NODE_CONTENTS, conf);
    putSavedValues(element, DISPATCH_NODE_OUTPUT, conf);
    putSavedValues(element, DISPATCH_NODE_CONSOLE, conf);
    putSavedValues(element, DISPATCH_NODE_ZIP_EXTRACT_ENABLE, conf);
    putSavedValues(element, HDS_LOGGER_ENABLE, conf);
    putSavedValues(element, HDS_LOGGER_JDBC_DB, conf);
    putSavedValues(element, HDS_LOGGER_JDBC_URL, conf);
    putSavedValues(element, HDS_LOGGER_JDBC_DRIVER, conf);
    putSavedValues(element, R_HOME, conf);
    putSavedValues(element, LD_LIBRARY_PATH, conf);
    putSavedValues(element, JRI_LIBRARY_PATH, conf);
    putSavedValues(element, JRI_LIBRARY_JAR_PATH, conf);
    putSavedValues(element, DISPATCH_YARN_JAR, conf);
    putSavedValues(element, DISPATCH_HDFS_SHARE_ACCOUNT, conf);
    putSavedValues(element, DRS_RM_CONFIG_LOCATION, conf);
    putSavedValues(element, YARN_RESOURCEMANAGER_WEB_ADDRESS, conf);
    putSavedValues(element, YARN_RESOURCEMANAGER_HA_ENABLE, conf);
    putSavedValues(element, YARN_RESOURCEMANAGER_HA_RM_IDS, conf);
    putSavedValues(element, YARN_RESOURCEMANAGER_ADDRESS, conf);
    putSavedValues(element, DRS_LOG_LEVEL, conf);
    putSavedValues(element, DRS_SCHEDULER_BATCH_SIZE, conf);
//    putSavedValues(element, DRS_JOB_QUEUE_NAME, conf);

    putSavedValues(element, DRS_FAIR_QUEUE_LIST, conf);
    putSavedValues(element, DRS_FAIR_QUEUE_WAIT_SIZE, conf);

    putSavedValues(element, ZOOKEEPER_QUORUM, conf);
    putSavedValues(element, ZOOKEEPER_SESSION_TIMEOUT, conf);

    //containerManager config
    putSavedValues(element, DRS_CONTAINERMANAGER_INTERVAL_COUNT, conf);
    putSavedValues(element, DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB, conf);
    putSavedValues(element, DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE, conf);
    putSavedValues(element, DRS_CONTAINERMANAGER_INTERVAL_BOUNDARY, conf);
    putSavedValues(element, DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST, conf);
    putSavedValues(element, DRS_RELEASE_IDLE_CONTAINER_ENABLE, conf);
    putSavedValues(element, DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE, conf);

    writeXMLtoFS(document, confXmlPath.toString());
    return confXmlPath;
  }

  private void putSavedValues(Element element, String name, DrsConfiguration conf) {
    if (conf.get(name) != null) {
      addPropertyToXml(element, name, conf.get(name));
    }
  }

  private void addPropertyToXml(Element element, String name, String value) {
    Element property = element.addElement("property");
    Element Name = property.addElement("name");
    Name.setText(name);
    Element Value = property.addElement("value");
    Value.setText(value);
  }

  private void writeXMLtoFS(Document document, String outputPath) throws IOException, UnsupportedEncodingException {
    OutputFormat format = OutputFormat.createPrettyPrint();
    XMLWriter writer = new XMLWriter(new FileOutputStream(new File(outputPath)), format);
    writer.write(document);
    writer.flush();
    writer.close();
  }

  public String getRmHttpAddressPort() {
    return workload.getStringValue(Workload.RM_HTTP_ADDRESS);
  }

  public int getUserCount() {
    return workload.getIntValue(Workload.USERCOUNT);
  }

  public int getFileCount() {
    return workload.getIntValue(Workload.FILECOUNT);
  }

  public int getMaxFleNumber() {
    return workload.getIntValue(Workload.FILENUMBER_MAX);
  }

  public int getMinFleNumber() {
    return workload.getIntValue(Workload.FILENUMBER_MIN);
  }

  public int getFleSize() {
    return workload.getIntValue(Workload.FILESIZE);
  }

  public String getFileSizeDistribution() {
    return workload.getStringValue(Workload.FILESIZE_DISTRIBUTION);
  }

  public String getFileCopyMethod() {
    return workload.getStringValue(FILE_COPY_METHOD);
  }

  public String getCopyFolder() {
    return workload.getStringValue(FILE_COPY_FROM);
  }

  public String getRemoteTempFolder() {
    return workload.getStringValue(Workload.REMOTE_TEMP_FOLDER);
  }

  public String getFileExtension() {
    return workload.getStringValue(Workload.FILE_EXTENSION);
  }

  public Level getLogLever() {
    String logLevel = workload.getStringValue(Workload.LOG_LEVEL);
    switch (logLevel) {
      case "warn":
        return Level.WARN;
      case "debug":
        return Level.DEBUG;
      case "error":
        return Level.ERROR;
      case "fatal":
        return Level.FATAL;
      default:
        return Level.INFO;
    }
  }

  public boolean getIsRunDrsAsync() {
    if ("true".equals(workload.getStringValue(IS_DRS_ASYNC))) {
      return true;
    }
    return false;
  }

  public int getRequestContainerSizeMb() {
    return simulaterConf.getInt(CONTAINER_MEMORY_MB, CONTAINER_MEMORY_MB_DEFAULT);
  }

  public int getIntervalMinMemSize() {
    return simulaterConf.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB_DEFAULT);
  }

  public int getMultiple() {
    return simulaterConf.getInt(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE_DEFAULT);
  }

  public String getRequestContainerSizeSet() {
    return simulaterConf.get(DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST,
            DrsConfiguration.DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST_DEFAULT);
  }

  public int getRequestContainerVcores() {
    return simulaterConf.getInt(CONTAINER_CPU_VCORES, CONTAINER_CPU_VCORES_DEFAULT);
  }

  public void checkConf() {
    //    "remote.temp.folder" 要確認尾部是/
    String remoteFolder = getRemoteTempFolder();
    if (remoteFolder != null && !remoteFolder.endsWith(File.separator)) {
      workload.setValue(Workload.REMOTE_TEMP_FOLDER, remoteFolder + File.separator);
    }
    String copyto = workload.getStringValue(Workload.COPYTO);
    if (copyto != null && !copyto.endsWith(File.separator)) {
      workload.setValue(Workload.COPYTO, copyto + File.separator);
    }

  }

}
