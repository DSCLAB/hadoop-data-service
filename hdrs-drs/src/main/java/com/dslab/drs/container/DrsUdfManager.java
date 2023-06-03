package com.dslab.drs.container;

import com.dslab.drs.exception.JRIException;
import com.dslab.drs.utils.DrsConfiguration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.rosuda.JRI.Rengine;

/**
 *
 * @author caca
 */
public class DrsUdfManager {
  private static final Log LOG = LogFactory.getLog(DrsUdfManager.class);
  private YarnConfiguration conf;
  private String beforeAddress = "";
  private String afterAddress = "";
  private boolean hasbeforeStage = false;
  private boolean hasafterStage = false;
  private String beforeFileName = "";
  private String afterFileName = "";
  private Rengine engine;

  public DrsUdfManager(YarnConfiguration conf, Rengine engine) {
    this.conf = conf;
    this.engine = engine;
  }

  public DrsUdfManager(YarnConfiguration conf) {
    this.conf = conf;
  }

  public void init() {
    String UDFResource_before = conf.get(
            DrsConfiguration.DRS_UDF_EXECUTE_BEFORECODE,
            DrsConfiguration.DRS_UDF_EXECUTE_BEFORECODE_DEFAULT);
    String UDFResource_after = conf.get(
            DrsConfiguration.DRS_UDF_EXECUTE_AFTERCODE, 
            DrsConfiguration.DRS_UDF_EXECUTE_AFTERCODE_DEFAULT);

    LOG.info("Get \"" + DrsConfiguration.DRS_UDF_EXECUTE_BEFORECODE + "\":"+ UDFResource_before);
    LOG.info("Get \"" + DrsConfiguration.DRS_UDF_EXECUTE_AFTERCODE + "\":"+ UDFResource_after);

    if (!UDFResource_before.equals("")) {
      String fileName = FilenameUtils.getName(UDFResource_before);
      checkUdfStageBeforeCode(UDFResource_before);
    }
    if (!UDFResource_after.equals("")) {
      String fileName = FilenameUtils.getName(UDFResource_after);
      checkUdfStageAfterCode(UDFResource_after);
    }
  }

  public boolean executeBeforeCode(Rengine engine) {
    return JRIExecutor.sourceRscript(engine, beforeFileName);
  }

  public boolean executeAfterCode(Rengine engine) {
    return JRIExecutor.sourceRscript(engine, afterFileName);
  }

  public boolean hasUdfBeforeCode() {
    return hasbeforeStage;
  }

  public boolean hasUdfAfterCode() {
    return hasafterStage;
  }

  public boolean isUdfBeforeCodeFromRemote() {
    return beforeAddress.contains("://");
  }

  public boolean isUdfAfterCodeFromRemote() {
    return afterAddress.contains("://");
  }

  public void checkUdfStageBeforeCode(String part) {
    hasbeforeStage = true;
    beforeAddress = part;
    beforeFileName = FilenameUtils.getName(this.beforeAddress);
    if (!isUdfBeforeCodeFromRemote()) {
      beforeAddress = conf.get(DrsConfiguration.DRS_UDF_DIR,DrsConfiguration.DRS_UDF_DIR_DEFAULT).concat("/").concat(beforeAddress);
    }
  }

  public void checkUdfStageAfterCode(String part) {
    hasafterStage = true;
    afterAddress = part;
    afterFileName = FilenameUtils.getName(this.afterAddress);
    if (!isUdfAfterCodeFromRemote()) {
      afterAddress = conf.get(DrsConfiguration.DRS_UDF_DIR,DrsConfiguration.DRS_UDF_DIR_DEFAULT).concat("/").concat(afterAddress);
    }
  }

  public String getBeforeAddress() {
    return this.beforeAddress;
  }

  public String getAfterAddress() {
    return this.afterAddress;
  }

  public String getBeforeFileName() {
    return beforeFileName;
  }

  public String getAfterFileName() {
    return afterFileName;
  }

  public Exception getException(Rengine engine) {
    return new JRIException(JRIExecutor.getRerrMessage(engine));
  }
}
