package com.dslab.drs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RegisterMessage implements Writable {

	private String message;
	private String nodeName;
	private String containerId;

	public RegisterMessage() {
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}

	public String getMessage() {
		return message;
	}

	public String getNodeName() {
		return nodeName;
	}

	public String getContainerId() {
		return containerId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(message.length());
		out.writeBytes(message);
		out.writeInt(nodeName.length());
		out.writeBytes(nodeName);
		out.writeInt(containerId.length());
		out.writeBytes(containerId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		message = WritableUtils.readString(in);
		nodeName = WritableUtils.readString(in);
		containerId = WritableUtils.readString(in);
	}

}
