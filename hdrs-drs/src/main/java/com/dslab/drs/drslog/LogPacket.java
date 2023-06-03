package com.dslab.drs.drslog;

import java.util.List;

/**
 *
 * @author kh87313
 */
public final class LogPacket implements java.io.Serializable {

  private final List<DrsPhaseLog> phaseList;
  private final List<DrsResourceLog> resourceList;

  public LogPacket(List<DrsPhaseLog> phaseList, List<DrsResourceLog> resourceList) {
    this.phaseList = phaseList;
    this.resourceList = resourceList;
  }

  public List<DrsPhaseLog> getPhaseList() {
    return phaseList;
  }

  public List<DrsResourceLog> getResourceList() {
    return resourceList;
  }
  
  public int getToalCellSize(){
    return phaseList.size()+resourceList.size();
  }

}
