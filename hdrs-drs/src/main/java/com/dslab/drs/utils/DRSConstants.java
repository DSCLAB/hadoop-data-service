package com.dslab.drs.utils;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 *
 * @author caca
 */
public class DRSConstants {

  public final static String DISPATCH_NODE_SCRIPT_LOG = "dispatch.node.script.log";
  public final static String DISPATCH_NODE_FILES = "dispatch.node.files";
  public final static String DISPATCH_NODE_CONTENTS = "dispatch.node.contents";
  public final static String DISPATCH_NODE_CONSOLE = "dispatch.node.console";
  public final static String DISPATCH_NODE_OUTPUT = "dispatch.node.output";
  public final static String DISPATCH_NODE_ZIP_EXTRACT_ENABLE = "dispatch.node.zip.extract.enable";

  public final static String DISPATCH_HDS_ACCESS_ADDRESS = "dispatch.HDS.access.address";

  public final static String DISPATCH_SCRIPT_INTPUT_PATH = "dispatch.script.input.path";

  public final static String DISPATCH_SCRIPT_OUTPUT_LIST = "dispatch.script.output.path.list";
  public final static String DISPATCH_SCRIPT_OUTPUT_PATH = "dispatch.script.output.path";
  public final static String DISPATCH_SCRIPT_ENVIRONMENT_VALUE = "dispatch.script.environment.value";

  public final static String DRS_UDF_DIR = "drs.UDF.dir";
  public final static String DRS_UDF_EXECUTE = "drs.UDF.execute";
  public final static String DRS_UDF_EXECUTE_BEFORECODE = "drs.UDF.execute.BeforeCode";
  public final static String DRS_UDF_EXECUTE_AFTERCODE = "drs.UDF.execute.AfterCode";
  public final static String DRS_UDF_STAGE_BEFORE = "BeforeCode";
  public final static String DRS_UDF_STAGE_AFTER = "AfterCode";
  public final static String RCONSOLE_PREFIX = "RConsole.";
  public static String JRI_Client_Record_Message = "execute normally";
  public final static String DISPATCH_SCHEDULER = "dispatch.scheduler";
  public final static String DISPATCH_LOCALITY_SCHEDULER_THRESHOLD = "dispatch.LocalityScheduler.threshold";
  public final static String DISPATCH_LOCALITY_SCHEDULER_TIMEOUT = "dispatch.LocalityScheduler.timeout";
  public final static String DISPATCH_LOCALITY_SCHEDULER_RATIO = "dispatch.locality.ratio";
  public final static String DRS_HDS_SPLITE_TOKEN = ",";
  public final static String DRS_HDS_AUTH_TOKEN = "drs.hds.auth.token";

  public final static String LOG_DB_PHASE_TABLE_NAME = "DRS_PHASE";
  public final static String LOG_DB_RESOURCE_TABLE_NAME = "DRS_RESOURCE";

  public static void setYarnDefaultValues(YarnConfiguration conf) {
    set(conf, DISPATCH_SCRIPT_OUTPUT_LIST, "DRS_output_dir_list");
    set(conf, DISPATCH_SCRIPT_OUTPUT_PATH, "DRS_output_dir_full_path");
    set(conf, DISPATCH_SCRIPT_INTPUT_PATH, "DRS_input_file_full_path");
    set(conf, DISPATCH_NODE_SCRIPT_LOG, "ErrorLogs");
    set(conf, DISPATCH_NODE_FILES, "HDS_Files");
    set(conf, DISPATCH_NODE_CONTENTS, "all_contents");
    set(conf, DISPATCH_NODE_OUTPUT, "Output");
    set(conf, DISPATCH_NODE_CONSOLE, "Console");
  }

  private static void set(YarnConfiguration conf, String name, String defaultValue) {
    if (conf.get(name) == null) {
      conf.set(name, defaultValue);
    }
  }

}
