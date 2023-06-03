package com.dslab.drs.api.async;

/**
 *
 * @author kh87313 copy form hadoop-2.6.0
 */
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
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
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;

@Private
@Unstable
public class AMRMClientAsyncImpl<T extends ContainerRequest>
        extends AMRMClientAsync<T> {

  private static final Log LOG = LogFactory.getLog(AMRMClientAsyncImpl.class);

  private final HeartbeatThread heartbeatThread;
  private final CallbackHandlerThread handlerThread;

  private final BlockingQueue<AllocateResponse> responseQueue;

  private final Object unregisterHeartbeatLock = new Object();

  private volatile boolean keepRunning;
  private volatile float progress;

  private volatile Throwable savedException;

  public AMRMClientAsyncImpl(int intervalMs, CallbackHandler callbackHandler) {
    this(new AMRMClientImpl<>(), intervalMs, callbackHandler);
  }

  @Private
  @VisibleForTesting
  public AMRMClientAsyncImpl(AMRMClient<T> client, int intervalMs,
          CallbackHandler callbackHandler) {
    super(client, intervalMs, callbackHandler);
    heartbeatThread = new HeartbeatThread();
    handlerThread = new CallbackHandlerThread();
    responseQueue = new LinkedBlockingQueue<>();
    keepRunning = true;
    savedException = null;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    client.init(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    handlerThread.setDaemon(true);
    handlerThread.start();
    client.start();
    super.serviceStart();
  }

  /**
   * Tells the heartbeat and handler threads to stop and waits for them to
   * terminate.
   */
  @Override
  protected void serviceStop() throws Exception {
    keepRunning = false;
    heartbeatThread.interrupt();
    try {
      heartbeatThread.join();
    } catch (InterruptedException ex) {
      LOG.error("Error joining with heartbeat thread", ex);
    }
    client.stop();
    handlerThread.interrupt();
    super.serviceStop();
  }

  public void setHeartbeatInterval(int interval) {
    heartbeatIntervalMs.set(interval);
  }

  public List<? extends Collection<T>> getMatchingRequests(
          Priority priority,
          String resourceName,
          Resource capability) {
    return client.getMatchingRequests(priority, resourceName, capability);
  }

  public RegisterApplicationMasterResponse registerApplicationMaster(
          String appHostName, int appHostPort, String appTrackingUrl)
          throws YarnException, IOException {
    RegisterApplicationMasterResponse response = client
            .registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);
    heartbeatThread.start();
    return response;
  }

  public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
          String appMessage, String appTrackingUrl) throws YarnException,
          IOException {
    synchronized (unregisterHeartbeatLock) {
      keepRunning = false;
      client.unregisterApplicationMaster(appStatus, appMessage, appTrackingUrl);
    }
  }

  public void addContainerRequest(T req) {
    client.addContainerRequest(req);
  }

  public void removeContainerRequest(T req) {
    client.removeContainerRequest(req);
  }

  public void releaseAssignedContainer(ContainerId containerId) {
    client.releaseAssignedContainer(containerId);
  }

  public Resource getAvailableResources() {
    return client.getAvailableResources();
  }

  public int getClusterNodeCount() {
    return client.getClusterNodeCount();
  }

  private class HeartbeatThread extends Thread {

    public HeartbeatThread() {
      super("AMRM Heartbeater thread");
    }

    public void run() {
      while (true) {
        AllocateResponse response = null;
        // synchronization ensures we don't send heartbeats after unregistering
        synchronized (unregisterHeartbeatLock) {
          if (!keepRunning) {
            return;
          }

          try {
            response = client.allocate(progress);
          } catch (ApplicationAttemptNotFoundException e) {
            handler.onShutdownRequest();
            LOG.info("Shutdown requested. Stopping callback.");
            return;
          } catch (Throwable ex) {
            LOG.error("Exception on heartbeat", ex);
            savedException = ex;
            // interrupt handler thread in case it waiting on the queue
            handlerThread.interrupt();
            return;
          }
          if (response != null) {
            while (true) {
              try {
                responseQueue.put(response);
                break;
              } catch (InterruptedException ex) {
                LOG.debug("Interrupted while waiting to put on response queue", ex);
              }
            }
          }
        }
        try {
          TimeUnit.MILLISECONDS.sleep(heartbeatIntervalMs.get());
        } catch (InterruptedException ex) {
          LOG.debug("Heartbeater interrupted", ex);
        }
      }
    }
  }

  private class CallbackHandlerThread extends Thread {

    public CallbackHandlerThread() {
      super("AMRM Callback Handler Thread");
    }

    public void run() {
      while (true) {
        if (!keepRunning) {
          return;
        }
        try {
          AllocateResponse response;
          if (savedException != null) {
            LOG.error("Stopping callback due to: ", savedException);
            handler.onError(savedException);
            return;
          }
          try {
            response = responseQueue.take();
          } catch (InterruptedException ex) {
            LOG.info("Interrupted while waiting for queue", ex);
            continue;
          }
          List<NodeReport> updatedNodes = response.getUpdatedNodes();
          if (!updatedNodes.isEmpty()) {
            handler.onNodesUpdated(updatedNodes);
          }

          List<ContainerStatus> completed
                  = response.getCompletedContainersStatuses();
          if (!completed.isEmpty()) {
            handler.onContainersCompleted(completed);
          }

          List<Container> allocated = response.getAllocatedContainers();
          if (!allocated.isEmpty()) {
            handler.onContainersAllocated(allocated);
          }

          PreemptionMessage preemptionMessage = response.getPreemptionMessage();
          if (preemptionMessage != null) {
            handler.onPreemption(preemptionMessage);
          }

          progress = handler.getProgress();
        } catch (Throwable ex) {
          handler.onError(ex);
          // re-throw exception to end the thread
          throw new YarnRuntimeException(ex);
        }
      }
    }
  }
}
