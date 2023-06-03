package com.dslab.drs.yarn.application;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NMCallbackHandler implements NMClientAsync.CallbackHandler {


  private static final Log LOG = LogFactory.getLog(NMCallbackHandler.class);
  private ConcurrentMap<ContainerId, Container> containers;
  private ApplicationMaster applicationMaster;

  public NMCallbackHandler() {
  }

  public NMCallbackHandler(ApplicationMaster applicationMaster) {
    this.applicationMaster = applicationMaster;
    this.containers = new ConcurrentHashMap<>();

  }

  public void addContainer(ContainerId containerId, Container container) {
    containers.putIfAbsent(containerId, container);
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {

    LOG.debug("onContainerStopped(): stop container");
    containers.remove(containerId);
    LOG.debug("Succeeded to stop Container "+ containerId);

  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    LOG.info("Container Status: id = " + containerId
            + ", status = " + containerStatus);
  }

  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    LOG.info("Succeeded to start Container " + containerId);

    Container container = containers.get(containerId);
    if (container != null) {
      applicationMaster.nmClient.getContainerStatusAsync(containerId, container.getNodeId());
    }
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to start Container " + containerId);
    containers.remove(containerId);
    applicationMaster.numCompletedContainers.incrementAndGet();
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to query the status of Container "+ containerId);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to stop Container "+ containerId);
    LOG.error("ErrorMessage: "+ t.getMessage());
    containers.remove(containerId);
  }

}
