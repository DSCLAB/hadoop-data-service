package com.dslab.drs.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Level;

/**
 *
 * @author kh87313
 */
public class DrsConfiguration extends YarnConfiguration {

  private static final Log LOG = LogFactory.getLog(DrsConfiguration.class);
  //來自yarn-dispatch.xml、或drs.xml
  public static final String DISPATCH_HDS_ACCESS_ADDRESS = "dispatch.HDS.access.address";
  public static final String DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT = "http://HOST-ADDRESS:8000/dataservice/v1/access";

  public static final String DISPATCH_HDS_LIST_ADDRESS = "dispatch.HDS.list.address";
  public static final String DISPATCH_HDS_LIST_ADDRESS_DEFAULT = "http://HOST-ADDRESS:8000/dataservice/v1/list?from=";

  public static final String DISPATCH_HDS_RUN_ADDRESS = "dispatch.HDS.run.address";
  public static final String DISPATCH_HDS_RUN_ADDRESS_DEFAULT = "http://HOST-ADDRESS:8000/dataservice/v1/run?";

  public static final String DISPATCH_HDS_KILL_ADDRESS = "dispatch.HDS.kill.address";
  public static final String DISPATCH_HDS_KILL_ADDRESS_DEFAULT = "http://HOST-ADDRESS:8000/dataservice/v1/killdrs?";
  //http://host/dataservice/v1/killdrs?

  public static final String DISPATCH_HDS_WATCH_DRS_ADDRESS = "dispatch.HDS.watch.drs.address";
  public static final String DISPATCH_HDS_WATCH_DRS_ADDRESS_DEFAULT = "http://HOST-ADDRESS:8000/dataservice/v1/watchdrs?";
  //http://host/dataservice/v1/watchdrs?

  public static final String DRS_SLAVES = "drs.slaves";

  public static final String DRS_LOG_LEVEL = "drs.log.level";
  public static final String DRS_LOG_LEVEL_DEFAULT = "info";

  public static final String DRS_JOB_QUEUE_NAME = "drs.job.queue.name";
  public static final String DRS_JOB_QUEUE_NAME_DEFAULT = "default";

  public static final String DRS_HDS_AUTH_TOKEN = "drs.hds.auth.token"; //TODO 這參數一定要在drs.xml設定，否則會沒辦法得到yarn-dispatch.xml，但就要所有DRS使用者都用同一個TOKEN
  public static final String DRS_HDS_AUTH_TOKEN_DEFAULT = "sweatshopsdslab";

  public static final String APPLICATION_MASTER_MEMORY = "application.master.memory"; //TODO 
  public static final int APPLICATION_MASTER_MEMORY_DEFAULT = 1024;

  public static final String APPLICATION_MAX_ATTEMPTS = "application.max.attempts"; //TODO
  public static final int APPLICATION_MAX_ATTEMPTS_DEFAULT = 2;

  public static final String APPLICATION_LAUNCH_CONTAINER_NUMBER = "application.launch.container.number";
  public static final int APPLICATION_LAUNCH_CONTAINER_NUMBER_DEFAULT = 5;

  public static final String APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN = "application.launch.container.number.min";
  public static final int APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN_DEFAULT = 5;

  public static final String CONTAINER_MEMORY_MB = "container.memory.mb";
  public static final int CONTAINER_MEMORY_MB_DEFAULT = 1024;

  public static final String CONTAINER_CPU_VCORES = "container.cpu.vcores";
  public static final int CONTAINER_CPU_VCORES_DEFAULT = 1;

  public static final String DRS_JVM_HEAPSIZE_RATIO = "drs.jvm.heapsize.ratio";
  public static final double DRS_JVM_HEAPSIZE_RATIO_DEFAULT = 0.8;

  public static final String CONTAINER_MAX_SUSPEND_TIME_SEC = "container.max.suspend.time.sec";
  public static final int CONTAINER_MAX_SUSPEND_TIME_SEC_DEFAULT = 120;//TODO  default值太小

  public static final String DRS_SCHEDULER_BATCH_SIZE = "drs.scheduler.batch.size";
  public static final int DRS_SCHEDULER_BATCH_SIZE_DEFAULT = 100;

  public static final String CONTAINER_MAX_RETRY_TIMES = "container.max.retry.times";
  public static final int CONTAINER_MAX_RETRY_TIMES_DEFAULT = 5;

  public static final String CONTAINER_TASK_RETRY_TIMES = "container.task.retry.times";
  public static final int CONTAINER_TASK_RETRY_TIMES_DEFAULT = 3;

  public static final String DRS_RELEASE_IDLE_CONTAINER_ENABLE = "drs.release.idle.container.enable";
  public static final boolean DRS_RELEASE_IDLE_CONTAINER_ENABLE_DEFAULT = true;

  public static final String DISPATCH_SCHEDULER = "dispatch.scheduler";
  public static final String DISPATCH_SCHEDULER_DEFAULT = "locality";

  public static final String DISPATCH_LOCALITYSCHEDULER_THRESHOLD = "dispatch.LocalityScheduler.threshold";
  public static final int DISPATCH_LOCALITYSCHEDULER_THRESHOLD_DEFAULT = 10000;

  public static final String DISPATCH_LOCALITYSCHEDULER_TIMEOUT = "dispatch.LocalityScheduler.timeout";
  public static final int DISPATCH_LOCALITYSCHEDULER_TIMEOUT_DEFAULT = 60;

  public static final String DISPATCH_LOCALITY_RATIO = "dispatch.locality.ratio";
  public static final double DISPATCH_LOCALITY_RATIO_DEFAULT = 0.3;

  public static final String DRS_UDF_DIR = "drs.UDF.dir"; //TODO 需要設定DEFAULT值嗎? 搞懂這是啥  //目前的使用方法貌似會導致有設定卻沒檔案
  public static final String DRS_UDF_DIR_DEFAULT = "/user/hbase"; //TODO 確認是否為BUG

  public static final String DRS_UDF_EXECUTE_BEFORECODE = "drs.UDF.execute.BeforeCode";
  public static final String DRS_UDF_EXECUTE_BEFORECODE_DEFAULT = "";//TODO 需要設定DEFAULT值嗎? 

  public static final String DRS_UDF_EXECUTE_AFTERCODE = "drs.UDF.execute.AfterCode";
  public static final String DRS_UDF_EXECUTE_AFTERCODE_DEFAULT = "";//TODO 需要設定DEFAULT值嗎? 

  public static final String DISPATCH_SCRIPT_INPUT_PATH = "dispatch.script.input.path";
  public static final String DISPATCH_SCRIPT_INPUT_PATH_DEFAULT = "DRS_input_file_full_path";

  public static final String DISPATCH_SCRIPT_OUTPUT_PATH = "dispatch.script.output.path";
  public static final String DISPATCH_SCRIPT_OUTPUT_PATH_DEFAULT = "DRS_output_dir_full_path";

  public static final String DISPATCH_SCRIPT_OUTPUT_PATH_LIST = "dispatch.script.output.path.list";
  public static final String DISPATCH_SCRIPT_OUTPUT_PATH_LIST_DEFAULT = "DRS_output_dir_list";

  public static final String DISPATCH_SCRIPT_ENVIRONMENT_VALUE = "dispatch.script.environment.value";
  public static final String DISPATCH_SCRIPT_ENVIRONMENT_VALUE_DEFAULT = "";

  public static final String DISPATCH_NODE_SCRIPT_LOG = "dispatch.node.script.log";
  public static final String DISPATCH_NODE_SCRIPT_LOG_DEFAULT = "ErrorLogs";

  public static final String DISPATCH_NODE_FILES = "dispatch.node.files";
  public static final String DISPATCH_NODE_FILES_DEFAULT = "HDS_Files";

  public static final String DISPATCH_NODE_CONTENTS = "dispatch.node.contents";
  public static final String DISPATCH_NODE_CONTENTS_DEFAULT = "all_contents";

  public static final String DISPATCH_NODE_OUTPUT = "dispatch.node.output";
  public static final String DISPATCH_NODE_NODE_DEFAULT = "Output";

  public static final String DISPATCH_NODE_CONSOLE = "dispatch.node.console";
  public static final String DISPATCH_NODE_CONSOLE_DEFAULT = "Console";

  public static final String DISPATCH_NODE_ZIP_EXTRACT_ENABLE = "dispatch.node.zip.extract.enable";
  public static final String DISPATCH_NODE_ZIP_EXTRACT_ENABLE_DEFAULT = "false";

  public static final String HDS_LOGGER_ENABLE = "hds.logger.enable";
  public static final String HDS_LOGGER_ENABLE_DEFAULT = "false";
  public static final String HDS_LOGGER_JDBC_DB = "hds.logger.jdbc.db";
  public static final String HDS_LOGGER_JDBC_URL = "hds.logger.jdbc.url";
  public static final String HDS_LOGGER_JDBC_DRIVER = "hds.logger.jdbc.driver";
  public static final String HDS_LOGGER_PERIOD = "hds.logger.period";
  public static final int HDS_LOGGER_PERIOD_DEFAULT = 2;
  public static final String HDS_LOGGER_QUEUE_SIZE = "hds.logger.queue.size";
  public static final int HDS_LOGGER_QUEUE_SIZE_DEFAULT = 1000;
  public static final String HDS_LOGGER_RETRY_TIME = "hds.logger.retry.times";
  public static final int HDS_LOGGER_RETRY_TIME_DEFAULT = 3;
  public static final String HDS_LOGGER_BATCH_SIZE = "hds.logger.batch.size";
  public static final int HDS_LOGGER_BATCH_SIZE_DEFAULT = 30;

  //Drs fair schedule queue
  public static final String DRS_FAIR_QUEUE_LIST = "drs.fair.queue.list";
  public static final String DRS_FAIR_QUEUE_LIST_DEFAULT = "default";
  public static final String DRS_FAIR_QUEUE_LIST_SPLITE_TOKEN = ",";

  public static final String DRS_FAIR_QUEUE_WAIT_SIZE = "drs.fair.queue.wait.size";
  public static final int DRS_FAIR_QUEUE_WAIT_SIZE_DEFAULT = 3;

//來自 drs.xml
  public static final String R_HOME = "R_HOME";  //TODO 改名
  public static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";  //TODO 改名
  public static final String JRI_LIBRARY_PATH = "JRI.library.path"; //TODO 改名
  public static final String JRI_LIBRARY_JAR_PATH = "JRI.library.jar.path"; //TODO 改名
  public static final String DISPATCH_YARN_JAR = "dispatch.yarn.jar";  //TODO 改名
  public static final String DISPATCH_HDFS_SHARE_ACCOUNT = "dispatch.hdfs.share.account";//TODO 改名

  public static final String DRS_RM_CONFIG_LOCATION = "drs.rm.config.location";
  public static final String DRS_RM_CONFIG_LOCATION_DEFAULT = "/etc/hadoop/conf.cloudera.yarn/";

  public static final String DRS_HBASE_CONFIG_LOCATION = "drs.hbase.config.location";
  public static final String DRS_HBASE_CONFIG_LOCATION_DEFAULT = "/etc/hbase/conf.cloudera.hbase/hbase-site.xml";

  public static final String YARN_RESOURCEMANAGER_WEB_ADDRESS = "yarn.resourcemanager.webapp.address";
  public static final String YARN_RESOURCEMANAGER_HA_ENABLE = "yarn.resourcemanager.ha.enabled";
  public static final String YARN_RESOURCEMANAGER_HA_RM_IDS = "yarn.resourcemanager.ha.rm-ids";

  public static final String YARN_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS = "yarn.resourcemanager.connect.max-wait.ms";

//來自 yarn-site.xml 
  public static final String YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address";

//constant
  public final static String LOG_DB_PHASE_TABLE_NAME = "DRS_PHASE";
  public final static String LOG_DB_RESOURCE_TABLE_NAME = "DRS_RESOURCE";
  public final static String DRS_HDS_SPLITE_TOKEN = ",";
  public final static String R_CONSOLE_PREFIX = "RConsole.";
  public final static String APP_NAME = "DRS";
  public final static int AM_CHECK_INTERVAL_MS = 1000;
  public final static boolean RELAX_LOCALITY_DEFAULT = true;

  //zookeeper constant
  public static final String ZK_DRS_ROOT_DIR = "/drs";
  public static final String ZK_DRS_WAIT_DIR_NAME = "blocked";

  public static final String ZOOKEEPER_QUORUM = "zookeeper.quorum";
  public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";
  public static final int ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 900000;

  //containerManager
  public static final String DRS_CONTAINERMANAGER_INTERVAL_COUNT = "drs.containermanager.interval-count";
  public static final int DRS_CONTAINERMANAGER_INTERVAL_COUNT_DEFAULT = 1;

  public static final String DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB = "drs.containermanager.interval.minimum.memory-mb";
  public static final int DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB_DEFAULT = 1024;

  public static final String DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE = "drs.containermanager.interval.increment.multiple";
  public static final int DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE_DEFAULT = 2;

  public static final String DRS_CONTAINERMANAGER_INTERVAL_BOUNDARY = "drs.containermanager.interval.boundary";

  public static final String DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST = "drs.containermanager.interval.container-count.list";
  public static final String DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST_DEFAULT = "5";

  public static final String DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE = "drs.containermanager.interval.resize.enable";
  public static final String DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE_DEFAULT = "false";

  public static final String DRS_CONTAINERMANAGER_INTERBAL_CONTAINER_RESIZE_ENABLE = "drs.containermanager.interval.container.resize.enable";
  public static final String DRS_CONTAINERMANAGER_INTERBAL_CONTAINER_RESIZE_ENABLE_DEFAULT = "false";

  public static final String DRS_CONTAINERMANAGER_RESIZE_POLICY = "drs.containermanager.resize.policy";
  public static final String DRS_CONTAINERMANAGER_RESIZE_POLICY_DEFAULT = "DEFAULT";

  public static final String DRS_CONTAINERMANAGER_CYCLIC_POLICY_CYCLETIME_MS = "drs.containermanager.cyclical.policy.cycletime.ms";
  public static final int DRS_CONTAINERMANAGER_CYCLIC_POLICY_CYCLETIME_MS_DEFAULT = 30000;

  public static final String DRS_CONTAINERMANAGER_OBJFUNCTION_VALUE = "drs.containermanager.objectfunction.value";
  public static final double DRS_CONTAINERMANAGER_OBJFUNCTION_VALUE_DEFAULT = 0.2;

  public static final String DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT
          = "drs.containermanager.default.policy.hastasktime.interval.count";
  public static final int DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT_DEFAULT = 1;

  private DrsConfiguration() {

  }

  public DrsConfiguration(Configuration conf) {
    this.addResource(conf);
  }

  public static DrsConfiguration newDrsConfiguration() {
//    setDrsDefaultValue(conf);
    return new DrsConfiguration();
  }

  public static void setDrsDefaultValue(DrsConfiguration conf) {
    setConf(conf, DISPATCH_HDS_ACCESS_ADDRESS, DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT);
    setConf(conf, DISPATCH_HDS_LIST_ADDRESS, DISPATCH_HDS_LIST_ADDRESS_DEFAULT);
    setConf(conf, DRS_HDS_AUTH_TOKEN, DRS_HDS_AUTH_TOKEN_DEFAULT);
    setConf(conf, APPLICATION_MASTER_MEMORY, APPLICATION_MASTER_MEMORY_DEFAULT);
    setConf(conf, APPLICATION_MAX_ATTEMPTS, APPLICATION_MAX_ATTEMPTS_DEFAULT);

    setConf(conf, APPLICATION_LAUNCH_CONTAINER_NUMBER, APPLICATION_LAUNCH_CONTAINER_NUMBER_DEFAULT);
    setConf(conf, APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN, APPLICATION_LAUNCH_CONTAINER_NUMBER_MIN_DEFAULT);
    setConf(conf, CONTAINER_MEMORY_MB, CONTAINER_MEMORY_MB_DEFAULT);
    setConf(conf, CONTAINER_CPU_VCORES, CONTAINER_CPU_VCORES_DEFAULT);
    setConf(conf, DRS_JVM_HEAPSIZE_RATIO, DRS_JVM_HEAPSIZE_RATIO_DEFAULT);
    setConf(conf, CONTAINER_MAX_SUSPEND_TIME_SEC, CONTAINER_MAX_SUSPEND_TIME_SEC_DEFAULT);
    setConf(conf, CONTAINER_MAX_RETRY_TIMES, CONTAINER_MAX_RETRY_TIMES_DEFAULT);
    setConf(conf, CONTAINER_TASK_RETRY_TIMES, CONTAINER_TASK_RETRY_TIMES_DEFAULT);
    setConf(conf, DISPATCH_SCHEDULER, DISPATCH_SCHEDULER_DEFAULT);
    setConf(conf, DISPATCH_LOCALITYSCHEDULER_THRESHOLD, DISPATCH_LOCALITYSCHEDULER_THRESHOLD_DEFAULT);
    setConf(conf, DISPATCH_LOCALITYSCHEDULER_TIMEOUT, DISPATCH_LOCALITYSCHEDULER_TIMEOUT_DEFAULT);
    setConf(conf, DISPATCH_LOCALITY_RATIO, DISPATCH_LOCALITY_RATIO_DEFAULT);
    setConf(conf, DRS_UDF_DIR, DRS_UDF_DIR_DEFAULT);
    setConf(conf, DRS_UDF_EXECUTE_BEFORECODE, DRS_UDF_EXECUTE_BEFORECODE_DEFAULT);
    setConf(conf, DRS_UDF_EXECUTE_AFTERCODE, DRS_UDF_EXECUTE_AFTERCODE_DEFAULT);
    setConf(conf, DISPATCH_SCRIPT_INPUT_PATH, DISPATCH_SCRIPT_INPUT_PATH_DEFAULT);
    setConf(conf, DISPATCH_SCRIPT_OUTPUT_PATH, DISPATCH_SCRIPT_OUTPUT_PATH_DEFAULT);
    setConf(conf, DISPATCH_SCRIPT_OUTPUT_PATH_LIST, DISPATCH_SCRIPT_OUTPUT_PATH_LIST_DEFAULT);
    setConf(conf, DISPATCH_SCRIPT_ENVIRONMENT_VALUE, DISPATCH_SCRIPT_ENVIRONMENT_VALUE_DEFAULT);
    setConf(conf, DISPATCH_NODE_SCRIPT_LOG, DISPATCH_NODE_SCRIPT_LOG_DEFAULT);
    setConf(conf, DISPATCH_NODE_FILES, DISPATCH_NODE_FILES_DEFAULT);
    setConf(conf, DISPATCH_NODE_CONTENTS, DISPATCH_NODE_CONTENTS_DEFAULT);
    setConf(conf, DISPATCH_NODE_OUTPUT, DISPATCH_NODE_NODE_DEFAULT);
    setConf(conf, DISPATCH_NODE_CONSOLE, DISPATCH_NODE_CONSOLE_DEFAULT);
    setConf(conf, DISPATCH_NODE_ZIP_EXTRACT_ENABLE, DISPATCH_NODE_ZIP_EXTRACT_ENABLE_DEFAULT);
    setConf(conf, HDS_LOGGER_ENABLE, HDS_LOGGER_ENABLE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE, DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE_DEFAULT);
    setConf(conf, HDS_LOGGER_PERIOD, HDS_LOGGER_PERIOD_DEFAULT);
    setConf(conf, HDS_LOGGER_QUEUE_SIZE, HDS_LOGGER_QUEUE_SIZE_DEFAULT);
    setConf(conf, HDS_LOGGER_RETRY_TIME, HDS_LOGGER_RETRY_TIME_DEFAULT);
    setConf(conf, HDS_LOGGER_BATCH_SIZE, HDS_LOGGER_BATCH_SIZE_DEFAULT);
    setConf(conf, DRS_RM_CONFIG_LOCATION, DRS_RM_CONFIG_LOCATION_DEFAULT);
    setConf(conf, DRS_SCHEDULER_BATCH_SIZE, DRS_SCHEDULER_BATCH_SIZE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERVAL_COUNT, DRS_CONTAINERMANAGER_INTERVAL_COUNT_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB, DRS_CONTAINERMANAGER_INTERVAL_MINIMUM_MEMORY_MB_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE, DRS_CONTAINERMANAGER_INTERVAL_INCREMENT_MULTIPLE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST, DRS_CONTAINERMANAGER_INTERVAL_CONTAINER_COUNT_LIST_DEFAULT);
    setConf(conf, DRS_RELEASE_IDLE_CONTAINER_ENABLE, DRS_RELEASE_IDLE_CONTAINER_ENABLE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE, DRS_CONTAINERMANAGER_INTERBAL_RESIZE_ENABLE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_INTERBAL_CONTAINER_RESIZE_ENABLE, DRS_CONTAINERMANAGER_INTERBAL_CONTAINER_RESIZE_ENABLE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_RESIZE_POLICY, DRS_CONTAINERMANAGER_RESIZE_POLICY_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_CYCLIC_POLICY_CYCLETIME_MS, DRS_CONTAINERMANAGER_CYCLIC_POLICY_CYCLETIME_MS_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_OBJFUNCTION_VALUE, DRS_CONTAINERMANAGER_OBJFUNCTION_VALUE_DEFAULT);
    setConf(conf, DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT, DRS_CONTAINERMANAGER_DEFAULT_POLICY_FINISHED_TASKTIME_COUNT_DEFAULT);
  }

  private static void setConf(DrsConfiguration conf, String name, String defaultValue) {
    if (conf.get(name) == null) {
      conf.set(name, defaultValue);
    }
  }

  private static void setConf(DrsConfiguration conf, String name, int defaultValue) {
    if (conf.get(name) == null) {
      conf.setInt(name, defaultValue);
    }
  }

  private static void setConf(DrsConfiguration conf, String name, double defaultValue) {
    if (conf.get(name) == null) {
      conf.setDouble(name, defaultValue);
    }
  }

  private static void setConf(DrsConfiguration conf, String name, boolean defaultValue) {
    if (conf.get(name) == null) {
      conf.setBoolean(name, defaultValue);
    }
  }

  public static void replaceHdsConfHost(DrsConfiguration conf, String hdsHostAddress) {
    String tempAccessAddress = conf.get(DISPATCH_HDS_ACCESS_ADDRESS, DISPATCH_HDS_ACCESS_ADDRESS_DEFAULT);
    String tempListAddress = conf.get(DISPATCH_HDS_LIST_ADDRESS, DISPATCH_HDS_LIST_ADDRESS_DEFAULT);
    String tempRunAddress = conf.get(DISPATCH_HDS_RUN_ADDRESS, DISPATCH_HDS_RUN_ADDRESS_DEFAULT);
    String tempKillAddress = conf.get(DISPATCH_HDS_KILL_ADDRESS, DISPATCH_HDS_KILL_ADDRESS_DEFAULT);
    String tempDrsWatchAddress = conf.get(DISPATCH_HDS_WATCH_DRS_ADDRESS, DISPATCH_HDS_WATCH_DRS_ADDRESS_DEFAULT);

    tempAccessAddress = tempAccessAddress.replaceAll("HOST-ADDRESS", hdsHostAddress);
    conf.set(DISPATCH_HDS_ACCESS_ADDRESS, tempAccessAddress);

    tempListAddress = tempListAddress.replaceAll("HOST-ADDRESS", hdsHostAddress);
    conf.set(DISPATCH_HDS_LIST_ADDRESS, tempListAddress);

    tempRunAddress = tempRunAddress.replaceAll("HOST-ADDRESS", hdsHostAddress);
    conf.set(DISPATCH_HDS_RUN_ADDRESS, tempRunAddress);

    tempKillAddress = tempKillAddress.replaceAll("HOST-ADDRESS", hdsHostAddress);
    conf.set(DISPATCH_HDS_KILL_ADDRESS, tempKillAddress);

    tempDrsWatchAddress = tempDrsWatchAddress.replaceAll("HOST-ADDRESS", hdsHostAddress);
    conf.set(DISPATCH_HDS_WATCH_DRS_ADDRESS, tempDrsWatchAddress);

  }

  public Level getLogLever() {
    String logLevel = this.get(DRS_LOG_LEVEL, DRS_LOG_LEVEL_DEFAULT);

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
}
