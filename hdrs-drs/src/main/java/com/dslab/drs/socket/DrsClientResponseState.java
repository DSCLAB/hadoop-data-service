package com.dslab.drs.socket;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class DrsClientResponseState implements ResponseState {

  private static final Log LOG = LogFactory.getLog(DrsClientResponseState.class);

  void updateTaskStatus(Socket m_socket, SocketGlobalVariables global) throws IOException {
    global.getServiceStatus().updateNodesStatus(global.getScheduler().getNodesStatus(),
            global.getScheduler().getProgress());
    ObjectOutputStream outToClient = new ObjectOutputStream(m_socket.getOutputStream());
    try {
      outToClient.writeObject(global.getServiceStatus().clone());
    } catch (CloneNotSupportedException ex) {
      LOG.error(ex);
      ex.printStackTrace();
    }
    outToClient.flush();
  }

}
