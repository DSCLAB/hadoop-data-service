package com.dslab.drs.yarn.application;

import com.dslab.drs.api.DRSClient;
import com.dslab.drs.exception.DrsArgumentsException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author caca
 */
public class ClientContext {

  private static final Log LOG = LogFactory.getLog(DRSClient.class);
  private final String code;
  private final String data;
  private final String config;
  private final String codeout;
  private final String copyto;
  private final String consoleto;

  public ClientContext(String code, String data, String config, String codeout, String copyto, String consoleto) {
    this.code = code;
    this.data = data;
    this.config = config;
    this.codeout = codeout;
    this.copyto = copyto;
    this.consoleto = consoleto;
  }

  //http://host/dataservice/v1/run?code=""&data=""&config=""&codeout=""&copyto=""
  public String getCode() {
    return code;
  }

  public String getData() {
    return data;
  }

  public String getConfig() {
    return config;
  }

  public String getCodeout() {
    return codeout;
  }

  public String getCopyto() {
    return copyto;
  }

  public String getConsoleto() {
    return consoleto;
  }

  public boolean isConsoleEnable() {
    return consoleto != null;
  }

  public void check() throws DrsArgumentsException {
    //check null
    ArrayList<String> nullArgs = new ArrayList<>();
    if (code == null || code.equals("")) {
      nullArgs.add("code");
    }
    if (data == null || data.equals("")) {
      nullArgs.add("data");
    }
    if (config == null || config.equals("")) {
      nullArgs.add("config");
    }
    if (codeout == null || codeout.equals("")) {
      nullArgs.add("codeout");
    }
    if (copyto == null || copyto.equals("")) {
      nullArgs.add("copyto");
    }

    if (nullArgs.size() > 0) {
      int count = 0;
      StringBuilder errorMessage = new StringBuilder("Argument:");
      for (String argv : nullArgs) {
        count++;
        if (count > 1) {
          errorMessage.append(",");
        }
        errorMessage.append(argv);
      }
      errorMessage.append(" need to be specified!");
      throw new DrsArgumentsException(errorMessage.toString());
    }

    //check codeou's length equal to copyto's length
    LOG.info("codeout:" + codeout);
    LOG.info("copyto:" + copyto);
    int codeoutNum = codeout.split(",").length;
    LOG.info("1," + codeoutNum);
    int copytoNum = copyto.split(",").length;
    LOG.info("2," + copytoNum);
    if (codeoutNum != copytoNum) {
      LOG.info("2.2");
      throw new DrsArgumentsException("Argument: codeout's path number not equal to copyto's path number");
    }

  }

}
