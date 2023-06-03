package com.dslab.drs.simulater.connection.resourceManager;

import com.dslab.drs.exception.GetJsonBodyException;
import com.dslab.drs.restful.api.json.JsonUtils;
import com.dslab.drs.restful.api.response.node.NodeResult;
import com.dslab.drs.restful.api.response.node.NodesBody;
import com.dslab.drs.restful.api.response.node.NodesResult;
import com.dslab.drs.restful.api.parser.Parser;
import com.dslab.drs.restful.api.response.appStatus.AppstateResult;
import com.dslab.drs.restful.api.response.application.AppResult;
import com.dslab.drs.restful.api.response.application.AppsBody;
import com.dslab.drs.restful.api.response.application.AppsResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kh87313
 */
public class RmRestfulApiRequestorImpl implements RmRestfulApiRequestor {

  private static final Log LOG = LogFactory.getLog(RmRestfulApiRequestorImpl.class);


//發現之前DRS是讀取yarn-dispatch.xml 不適合用在 simulator 內
//  //HA 環境下，Yarn HttpAddress和參數和非HA不一樣，嘗試連線確定新的
//  @Override
//  public String getRmhHttpAddressPort(DrsConfiguration conf) throws IOException {
//
//    String RM_HTTP_ADDRESS_PORT = conf.get(YARN_RESOURCEMANAGER_WEB_ADDRESS);
//    String RM_HA_ENABLED = conf.get(YARN_RESOURCEMANAGER_HA_ENABLE);
//    String RM_HA_RM_IDS = conf.get(YARN_RESOURCEMANAGER_HA_RM_IDS);
//    LOG.debug("RM_HTTP_ADDRESS_PORT:" + RM_HTTP_ADDRESS_PORT);
//    LOG.debug("RM_HA_ENABLED:" + RM_HA_ENABLED);
//    LOG.debug("RM_HA_RM_IDS:" + RM_HA_RM_IDS);
//
//    if ("true".equals(RM_HA_ENABLED)) {
//      if (RM_HA_RM_IDS != null) {
//        String[] rm_ids = RM_HA_RM_IDS.split(",");
//        String tempHttpAddressPosrt = "";
//        int connectionCount = 0;
//        for (int i = 0; i < rm_ids.length; i++) {
//          String rm_id = rm_ids[i];
//          tempHttpAddressPosrt = conf.get("yarn.resourcemanager.webapp.address." + rm_id);
//          try {
//            //連看看，看哪個才是RM的address
//            getNodesResult(tempHttpAddressPosrt);
//            break;
//          } catch (Exception ex) {
//            connectionCount++;
//            if (connectionCount == rm_ids.length) {
//              LOG.error("Can't connect to any host of yarn.resourcemanager.ha.rm-ids: " + RM_HA_RM_IDS);
//              throw new IOException("Can't connect to any host of yarn.resourcemanager.ha.rm-ids");
//            }
//          }
//        }
//        return tempHttpAddressPosrt;
//      } else {
//        throw new IOException("The value of yarn.resourcemanager.ha.rm-ids : null");
//      }
//    } else {
//      return RM_HTTP_ADDRESS_PORT;
//    }
//  }

  @Override
  public NodeResourceState getNodeResourceState(String rmHttpAddressPort) throws GetJsonBodyException {

    List<NodeResult> _nodeList = getNodesResult(rmHttpAddressPort);
    Set<String> nodes = new HashSet<>();
    int _availMemoryMB = 0;
    int _availVcores = 0;
    int _usedMemoryMB = 0;
    int _usedVcores = 0;

    for (NodeResult node : _nodeList) {
      nodes.add(node.getId());
      _availMemoryMB += node.getAvailMemoryMB();
      _availVcores += node.getAvailableVirtualCores();
      _usedMemoryMB += node.getUsedMemoryMB();
      _usedVcores += node.getUsedVirtualCores();
    }
    return new NodeResourceState(nodes, _availMemoryMB, _availVcores, _usedMemoryMB, _usedVcores);

  }

  @Override
  public Set<String> getRunningAppId(String rmHttpAddressPort) throws GetJsonBodyException {
    Set<String> runningAppId = new HashSet<>();
    List<AppResult> appsList = getAppResults(rmHttpAddressPort);
    appsList.stream().filter((app) -> ("ACCEPTED".equals(app.getState()) || "RUNNING".equals(app.getState())))
            .forEach((app) -> {
              runningAppId.add(app.getId());
            });
    return runningAppId;
  }

  @Override
  public Set<String> getAfterAppId(String rmHttpAddressPort, String lastApplicationId) throws GetJsonBodyException {
    if (lastApplicationId == null) {
      lastApplicationId = "application_0_0";
    }
    Set<String> After = new HashSet<>();
    List<AppResult> appList = getAppResults(rmHttpAddressPort);
    for (AppResult app : appList) {
      Optional<String> newerAppId = compareNewerApplicationId(lastApplicationId, app.getId());
      if (newerAppId.isPresent() && !lastApplicationId.equals(newerAppId.get())) {
        After.add(newerAppId.get());
      }
    }
    return After;
  }

  @Override
  public List<NodeResult> getNodesResult(String rmHttpAddressPort) throws GetJsonBodyException {
    String rmHttpNodesPath = "http://" + rmHttpAddressPort + "/ws/v1/cluster/nodes";
    String jsonStr = Parser.getJsonBodyFromUrl(rmHttpNodesPath);
    NodesBody nodesResult = JsonUtils.fromJson(jsonStr, NodesBody.class);

    if (nodesResult == null) {
      throw new GetJsonBodyException("Can't get nodesResult. ");
    }

    NodesResult nodes = nodesResult.getNodes();

    if (nodes == null) {
      throw new GetJsonBodyException("Can't get nodes info.");
    }

    List<NodeResult> nodeList = nodes.getNodes();

    if (nodeList == null) {
      throw new GetJsonBodyException("Can't get nodeList.");
    }
    return nodeList;
  }

  @Override
  public AppstateResult getApplicationStatus(String rmHttpAddressPort, String applicationId) throws GetJsonBodyException {
    String rmHttpNodesPath = "http://" + rmHttpAddressPort + "/ws/v1/cluster/apps/" + applicationId + "/state";
    String jsonStr = Parser.getJsonBodyFromUrl(rmHttpNodesPath);

    AppstateResult appResult = JsonUtils.fromJson(jsonStr, AppstateResult.class);

    if (appResult == null) {
      throw new GetJsonBodyException("Can't get application status from " + applicationId);
    }
    return appResult;
  }

  @Override
  public List<AppResult> getAppResults(String rmHttpAddressPort) throws GetJsonBodyException {
    String rmHttpNodesPath = "http://" + rmHttpAddressPort + "/ws/v1/cluster/apps";
    LOG.info("List apps info from " + rmHttpNodesPath);
    String jsonStr = Parser.getJsonBodyFromUrl(rmHttpNodesPath);
    LOG.debug("jsonStr:"+jsonStr);
    
    AppsBody appsBody = JsonUtils.fromJson(jsonStr, AppsBody.class);
    if (appsBody == null) {
      LOG.error("Can't get application status from RM Rest.");
      throw new GetJsonBodyException("Can't get applicayion result list");
    } 
    AppsResult appsResult =  appsBody.getApps();
    if(appsResult == null){
      return new ArrayList<>();
    }
    if (appsResult.getApp() == null) {
      LOG.warn("Can't get application list from RM Rest.");
      return new ArrayList<>();
    }
    return appsResult.getApp();
  }

  @Override
  public Optional<String> getLastApplicationId(String rmHttpAddressPort) throws GetJsonBodyException {
    Optional<String> currentLastApplicationId = Optional.empty();
    List<AppResult> appResults = getAppResults(rmHttpAddressPort);
    for (AppResult appResult : appResults) {
      if (!currentLastApplicationId.isPresent()) {
        currentLastApplicationId = Optional.of(appResult.getId());
      } else {
        Optional<String> compareResult = compareNewerApplicationId(currentLastApplicationId.get(), appResult.getId());
        if (compareResult.isPresent()) {
          currentLastApplicationId = compareResult;
        }
      }
    }

    return currentLastApplicationId;
  }

  public Optional<String> compareNewerApplicationId(String currentAppId, String nextAppId) {
    try {
      String[] splitCurrentApp = currentAppId.split("_");
      String[] nextCurrentApp = nextAppId.split("_");

      if (splitCurrentApp.length != 3 || nextCurrentApp.length != 3) {
        return Optional.empty();
      }

      long currentClusterStartTime = Long.parseLong(splitCurrentApp[1]);
      long nextClusterStartTime = Long.parseLong(nextCurrentApp[1]);

      long currentAppNumber = Long.parseLong(splitCurrentApp[2]);
      long nextAppNumber = Long.parseLong(nextCurrentApp[2]);

      if (currentClusterStartTime > nextClusterStartTime) {
        return Optional.of(currentAppId);
      } else if (currentClusterStartTime == nextClusterStartTime && currentAppNumber >= nextAppNumber) {
        return Optional.of(currentAppId);
      } else {
        return Optional.of(nextAppId);
      }
    } catch (NumberFormatException ex) {
      LOG.error(ex);
      return Optional.empty();
    }
  }
}
