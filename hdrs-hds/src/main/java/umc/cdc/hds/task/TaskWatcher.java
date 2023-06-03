/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.task;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.core.HBaseConnection;
import umc.cdc.hds.core.SingletonConnectionFactory;
import umc.udp.core.framework.AtomicCloseable;

/**
 *
 * @author brandboat
 */
public class TaskWatcher extends AtomicCloseable {

  private static final Log LOG = LogFactory.getLog(TaskWatcher.class);
  private final HBaseConnection conn;

  public TaskWatcher(final Configuration conf) throws Exception {
    conn = SingletonConnectionFactory.createConnection(conf);
  }

  @Override
  protected void internalClose() throws IOException {
  }

}
