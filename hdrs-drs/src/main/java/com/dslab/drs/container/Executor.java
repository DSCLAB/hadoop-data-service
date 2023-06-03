package com.dslab.drs.container;

import static com.dslab.drs.monitor.MonitorConstant.*;
import com.dslab.drs.yarn.application.ContainerGlobalVariable;


/**
 *
 * @author caca
 */
public abstract class Executor {

  boolean executeResult = true;
  String targetFile;

  final public ContainerTaskStatus execute() {
    ContainerGlobalVariable.RESOURCE_LOG_TYPE = R_INITIAL;
    executeResult = initial();

    if (executeResult) {
      ContainerGlobalVariable.RESOURCE_LOG_TYPE = R_GET_FILES;
      executeResult = prepareFiles();
    }
    if (executeResult) {
      ContainerGlobalVariable.RESOURCE_LOG_TYPE = R_COMPUTE;
      executeResult = main();
    }
    if (executeResult) {
      ContainerGlobalVariable.RESOURCE_LOG_TYPE = R_PUT_RESULTS;
      executeResult = outputFiles();
    }
    return getResult();
  }

  abstract boolean initial();

  abstract boolean prepareFiles();

  abstract boolean main();

  abstract boolean outputFiles();

  abstract ContainerTaskStatus getResult();

}
