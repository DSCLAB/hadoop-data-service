package com.dslab.drs.api.async;

/**
 *
 * @author kh87313 copy form hadoop-2.6.0
 */
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;

public abstract class AMRMClientAsync<T extends ContainerRequest> 
extends AbstractService {
  private static final Log LOG = LogFactory.getLog(AMRMClientAsync.class);
  
  protected final AMRMClient<T> client;
  protected final CallbackHandler handler;
  protected final AtomicInteger heartbeatIntervalMs = new AtomicInteger();

  public static <T extends ContainerRequest> AMRMClientAsync<T>
      createAMRMClientAsync(int intervalMs, CallbackHandler callbackHandler) {
    return new AMRMClientAsyncImpl<T>(intervalMs, callbackHandler);
  }
  
  public static <T extends ContainerRequest> AMRMClientAsync<T>
      createAMRMClientAsync(AMRMClient<T> client, int intervalMs,
          CallbackHandler callbackHandler) {
    return new AMRMClientAsyncImpl<T>(client, intervalMs, callbackHandler);
  }
  
  protected AMRMClientAsync(int intervalMs, CallbackHandler callbackHandler) {
    this(new AMRMClientImpl<T>(), intervalMs, callbackHandler);
  }
  
  @Private
  @VisibleForTesting
  protected AMRMClientAsync(AMRMClient<T> client, int intervalMs,
      CallbackHandler callbackHandler) {
    super(AMRMClientAsync.class.getName());
    this.client = client;
    this.heartbeatIntervalMs.set(intervalMs);
    this.handler = callbackHandler;
  }
    
  public void setHeartbeatInterval(int interval) {
    heartbeatIntervalMs.set(interval);
  }
  
  public abstract List<? extends Collection<T>> getMatchingRequests(
                                                   Priority priority, 
                                                   String resourceName, 
                                                   Resource capability);
  
  /**
   * Registers this application master with the resource manager. On successful
   * registration, starts the heartbeating thread.
   * @throws YarnException
   * @throws IOException
   */
  public abstract RegisterApplicationMasterResponse registerApplicationMaster(
      String appHostName, int appHostPort, String appTrackingUrl)
      throws YarnException, IOException;

  /**
   * Unregister the application master. This must be called in the end.
   * @param appStatus Success/Failure status of the master
   * @param appMessage Diagnostics message on failure
   * @param appTrackingUrl New URL to get master info
   * @throws YarnException
   * @throws IOException
   */
  public abstract void unregisterApplicationMaster(
      FinalApplicationStatus appStatus, String appMessage, String appTrackingUrl) 
  throws YarnException, IOException;

  /**
   * Request containers for resources before calling <code>allocate</code>
   * @param req Resource request
   */
  public abstract void addContainerRequest(T req);

  /**
   * Remove previous container request. The previous container request may have 
   * already been sent to the ResourceManager. So even after the remove request 
   * the app must be prepared to receive an allocation for the previous request 
   * even after the remove request
   * @param req Resource request
   */
  public abstract void removeContainerRequest(T req);

  public abstract void releaseAssignedContainer(ContainerId containerId);
  public abstract Resource getAvailableResources();
  public abstract int getClusterNodeCount();

  public void waitFor(Supplier<Boolean> check) throws InterruptedException {
    waitFor(check, 1000);
  }

  public void waitFor(Supplier<Boolean> check, int checkEveryMillis)
      throws InterruptedException {
    waitFor(check, checkEveryMillis, 1);
  };

  /**
   * Wait for <code>check</code> to return true for each
   * <code>checkEveryMillis</code> ms. In the main loop, this method will log
   * the message "waiting in main loop" for each <code>logInterval</code> times
   * iteration to confirm the thread is alive.
   * @param check user defined checker
   * @param checkEveryMillis interval to call <code>check</code>
   * @param logInterval interval to log for each
   */
  public void waitFor(Supplier<Boolean> check, int checkEveryMillis,
      int logInterval) throws InterruptedException {
    Preconditions.checkNotNull(check, "check should not be null");
    Preconditions.checkArgument(checkEveryMillis >= 0,
        "checkEveryMillis should be positive value");
    Preconditions.checkArgument(logInterval >= 0,
        "logInterval should be positive value");

    int loggingCounter = logInterval;
    do {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check the condition for main loop.");
      }

      boolean result = check.get();
      if (result) {
        LOG.info("Exits the main loop.");
        return;
      }
      if (--loggingCounter <= 0) {
        LOG.info("Waiting in main loop.");
        loggingCounter = logInterval;
      }

      Thread.sleep(checkEveryMillis);
    } while (true);
  }

  public interface CallbackHandler {
    
    public void onContainersCompleted(List<ContainerStatus> statuses);
    
    public void onContainersAllocated(List<Container> containers);

    public void onShutdownRequest();

    public void onNodesUpdated(List<NodeReport> updatedNodes);
    
    public float getProgress();

    public void onPreemption(PreemptionMessage preemptionMessage);
    
    public void onError(Throwable e);
  }
}
