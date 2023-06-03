package com.dslab.drs.yarn.application.containermanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CyclicalResizeAlgo extends ResizeAlgo {

  private static final Log LOG = LogFactory.getLog(CyclicalResizeAlgo.class);
  public static final String NAME = "CYCLICAL";
  private static final ContainerManagerImpl CMG = ContainerManagerImpl.getInstance();

  @Override
  public void execute() {
    LOG.info("start executing Cyclical policy.");
    execute.entrySet().stream().filter(e -> e.getValue() < 0)
            .forEach(e -> CMG.resizeContainer(new ContainerResizeArgs(e.getKey(), Math.abs(e.getValue()), true)));
    execute.entrySet().stream().filter(e -> e.getValue() > 0)
            .forEach(e -> CMG.resizeContainer(new ContainerResizeArgs(e.getKey(), e.getValue(), false)));
  }

}
