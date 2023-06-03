package com.dslab.drs.socket;

import com.dslab.drs.yarn.application.ServerToClientMessage;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public abstract class ContainerResponseState implements ResponseState {

  void outputMessage(ServerToClientMessage message, Socket m_socket) throws IOException {
    ObjectOutputStream outputStreamToClient = new ObjectOutputStream(m_socket.getOutputStream());
    outputStreamToClient.writeObject(message);
    outputStreamToClient.flush();
  }
}
