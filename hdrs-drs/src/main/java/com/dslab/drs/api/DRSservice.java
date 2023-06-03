package com.dslab.drs.api;

import java.io.IOException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 *
 * @author chen10
 */
public interface DRSservice {

  public void init(String code, String data, String config, String codeout, String copyto, String consoleto) throws IOException;

  public ApplicationId start() throws YarnException, IOException;

  public ServiceStatus getServiceStatus();

  public void abort();

  public boolean isAbort();

  public void close();

}
