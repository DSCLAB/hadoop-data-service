package com.dslab.drs.drslog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public final class LogStore {

  private static final Log LOG = LogFactory.getLog(LogStore.class);

  LinkedBlockingQueue<DrsPhaseLog> phaseQueue;
  LinkedBlockingQueue<DrsResourceLog> resourceQueue;

  private static LogStore INSTANCE;

  private LogStore() {
    phaseQueue = new LinkedBlockingQueue<>();
    resourceQueue = new LinkedBlockingQueue<>();
  }

  public static LogStore getLogStore() {
    if (INSTANCE == null) {
      INSTANCE = new LogStore();
    }
    return INSTANCE;
  }

  public synchronized void addPhaseLog(DrsPhaseLog phaseCell) {
    phaseQueue.add(phaseCell);
  }

  public synchronized void addResourceLog(DrsResourceLog resourceCell) {
    resourceQueue.add(resourceCell);
  }

  public synchronized LogPacket getLogPacket() {
    List<DrsPhaseLog> phaseList = new ArrayList<>();
    List<DrsResourceLog> resourceList = new ArrayList<>();
    DrsPhaseLog phaseLog;
    DrsResourceLog resourceLog;
    while ((phaseLog = phaseQueue.poll()) != null) {
      phaseList.add(phaseLog);
    }
    while ((resourceLog = resourceQueue.poll()) != null) {
      resourceList.add(resourceLog);
    }
    return new LogPacket(phaseList, resourceList);
  }
}
