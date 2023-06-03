/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.requestloadbalancer.RequestRedirectPlan;
import umc.cdc.hds.task.TaskInfoBuilder.TaskInfo;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author brandboat
 */
public class KillRedirect {

  private RequestRedirectPlan plan;
  private final ServerName serverName;
  private final Optional<String> id;

  public KillRedirect(Optional<String> id, ServerName serverName) throws IOException {
    this.serverName = serverName;
    this.id = id;
    this.plan = new RequestRedirectPlan(serverName);
    setRedirectPlan();
  }

  private void setRedirectPlan() throws IOException {
    Map<String, String> q = new HashMap<>();
    q.put(HDSConstants.TASK_ID, id.orElse(""));
    Query tq = new Query(q);
    CloseableIterator<TaskInfo> tis = TaskManager.getTaskInfo(tq);
    TaskInfo ti = tis.next();
    if (ti != null) {
      ServerName sn = ServerName.valueOf(
          ti.getServerName(), serverName.getPort(), 0);
      plan.setRedirectServer(sn);
    }
  }

  public RequestRedirectPlan getRedirectPlan() {
    return plan;
  }
}
