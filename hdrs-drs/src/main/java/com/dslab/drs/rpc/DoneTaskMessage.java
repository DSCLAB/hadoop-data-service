package com.dslab.drs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DoneTaskMessage implements Writable {

	public String message;
	public String nodeName;
	public String containerId;
	public String fileName;
	public String fileUrl;

	public DoneTaskMessage() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(message.length());
		out.writeBytes(message);
		out.writeInt(nodeName.length());
		out.writeBytes(nodeName);
		out.writeInt(containerId.length());
		out.writeBytes(containerId);
		out.writeInt(fileName.length());
		out.writeBytes(fileName);
		out.writeInt(fileUrl.length());
		out.writeBytes(fileUrl);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		message = WritableUtils.readString(in);
		nodeName = WritableUtils.readString(in);
		containerId = WritableUtils.readString(in);
		fileName = WritableUtils.readString(in);
		fileUrl = WritableUtils.readString(in);
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

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileUrl() {
		return fileUrl;
	}

	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}

}
