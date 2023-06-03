package com.dslab.drs.socket;

import com.dslab.drs.container.ContainerTaskStatus;
import java.net.Socket;

public class SocketRun {

  private int containerFinishCount = 0;
  private int register_count = 0;
  private int file_success_count = 0;
  private boolean DRSClientRunning = false;
  private boolean watchRequestFlag = false;
  private Socket m_socket;
  private String[] ask;
  private ContainerTaskStatus containerTaskStatus;

  public SocketRun() {
  }

  public ContainerTaskStatus getContainerTaskStatus() {
    return this.containerTaskStatus;
  }

  public int getContainerFinishCount() {
    return this.containerFinishCount;
  }

  public int getRegisterCount() {
    return this.register_count;
  }

  public int getFileSuccessCount() {
    return this.file_success_count;
  }

  public boolean getDRSClientRunning() {
    return this.DRSClientRunning;
  }

  public boolean getWatchRequestFlag() {
    return this.watchRequestFlag;
  }

  public Socket getSocket() {
    return this.m_socket;
  }

  public String[] getAsk() {
    return this.ask;
  }

  public void increaseContainerFinishCount() {
    this.containerFinishCount++;
  }

  public void increaseRegisterCount() {
    this.register_count++;
  }

  public void increaseFileSuccessCount() {
    this.file_success_count++;
  }

  public void setDRSClientRunning(boolean DRSClientRunning) {
    this.DRSClientRunning = DRSClientRunning;
  }

  public void setWatchRequestFlag(boolean watchRequestFlag) {
    this.watchRequestFlag = watchRequestFlag;
  }

  public void setSocket(Socket socket) {
    this.m_socket = socket;
  }

  public void setAsk(String[] ask) {
    this.ask = ask;
  }

  public void setContainerTaskStatus(ContainerTaskStatus containerTaskStatus) {
    this.containerTaskStatus = containerTaskStatus;
  }

}
