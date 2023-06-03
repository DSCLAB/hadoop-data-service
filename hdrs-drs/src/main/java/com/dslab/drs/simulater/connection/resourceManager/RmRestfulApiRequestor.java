package com.dslab.drs.simulater.connection.resourceManager;

import com.dslab.drs.exception.GetJsonBodyException;
import com.dslab.drs.restful.api.response.appStatus.AppstateResult;
import com.dslab.drs.restful.api.response.application.AppResult;
import com.dslab.drs.restful.api.response.node.NodeResult;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 *
 * @author kh87313
 */
public interface RmRestfulApiRequestor {

  public NodeResourceState getNodeResourceState(String rmHttpAddressPort) throws GetJsonBodyException;

  public Set<String> getRunningAppId(String rmHttpAddressPort) throws GetJsonBodyException;

  public Set<String> getAfterAppId(String rmHttpAddressPort,String lastApplicationId) throws GetJsonBodyException;

  public List<NodeResult> getNodesResult(String rmHttpAddressPort) throws GetJsonBodyException;

  public List<AppResult> getAppResults(String rmHttpAddressPort) throws GetJsonBodyException;
  
  public Optional<String> getLastApplicationId(String rmHttpAddressPort) throws GetJsonBodyException; //取的最後執行的Application Id

  public AppstateResult getApplicationStatus(String rmHttpAddressPort, String applicationId) throws GetJsonBodyException;
}
