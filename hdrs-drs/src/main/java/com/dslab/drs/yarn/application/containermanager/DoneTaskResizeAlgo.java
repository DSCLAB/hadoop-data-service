package com.dslab.drs.yarn.application.containermanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DoneTaskResizeAlgo extends ResizeAlgo {

  private static final Log LOG = LogFactory.getLog(DoneTaskResizeAlgo.class);
  public static final String NAME = "DONETASK";
  private static final ContainerManagerImpl CMG = ContainerManagerImpl.getInstance();

  @Override
  void execute() {
    LOG.info("start executing Donetask policy.");
    execute.entrySet().stream().filter(e -> e.getValue() < 0)
            .forEach(e -> CMG.resizeContainer(new ContainerResizeArgs(e.getKey(), Math.abs(e.getValue()), true)));
    execute.entrySet().stream().filter(e -> e.getValue() > 0)
            .forEach(e -> CMG.resizeContainer(new ContainerResizeArgs(e.getKey(), e.getValue(), false)));
  }

}
