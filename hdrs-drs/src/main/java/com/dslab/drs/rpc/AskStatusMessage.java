package com.dslab.drs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class AskStatusMessage implements Writable {

  private String message;
  private String nodeName;
  private String containerId;

  public AskStatusMessage() {
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(message.length());
    out.writeBytes(message);
    out.write(nodeName.length());
    out.writeBytes(nodeName);
    out.write(containerId.length());
    out.writeBytes(containerId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    message = WritableUtils.readString(in);
    nodeName = WritableUtils.readString(in);
    containerId = WritableUtils.readString(in);
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

}
