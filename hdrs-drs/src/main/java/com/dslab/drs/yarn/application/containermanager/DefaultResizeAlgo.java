/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.drs.yarn.application.containermanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class DefaultResizeAlgo extends ResizeAlgo {

  private static final Log LOG = LogFactory.getLog(DefaultResizeAlgo.class);
  public static final String NAME = "DEFAULT";
  private static final ContainerManagerImpl CMG = ContainerManagerImpl.getInstance();

  @Override
  void execute() {
    LOG.info("start executing Default policy.");
    execute.entrySet().stream().filter(e -> e.getValue() < 0)
            .forEach(e -> CMG.resizeContainer(new ContainerResizeArgs(e.getKey(), Math.abs(e.getValue()), true)));
    execute.entrySet().stream().filter(e -> e.getValue() > 0)
            .forEach(e -> CMG.resizeContainer(new ContainerResizeArgs(e.getKey(), e.getValue(), false)));
  }

}
