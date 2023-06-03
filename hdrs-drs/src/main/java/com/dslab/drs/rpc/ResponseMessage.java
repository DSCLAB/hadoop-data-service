package com.dslab.drs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class ResponseMessage implements Writable {
	public String message = "";
	public String fileUrl = "";
	public String fileName = "";
	private long fileSize = 0;

	public ResponseMessage() {
	}

	public ResponseMessage(String message) {
		this.message = message;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int num = in.readInt();
		if (num > 0) {
			message = WritableUtils.readString(in);
			num--;
		}
		if (num > 0) {
			fileUrl = WritableUtils.readString(in);
			num--;
		}
		if (num > 0) {
			fileName = WritableUtils.readString(in);
			num--;
		}
		if (num > 0) {
			fileSize = in.readLong();
			num--;
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		int i = numberOfProperty();
		out.writeInt(i);
		if (message.length() > 0) {
			out.writeInt(message.length());
			out.writeBytes(message);
		}
		if (fileUrl.length() > 0) {
			out.writeInt(fileUrl.length());
			out.writeBytes(fileUrl);
		}
		if (fileName.length() > 0) {
			out.writeInt(fileName.length());
			out.writeBytes(fileName);
		}
		if (fileSize > 0) {
			out.writeLong((int) fileSize);
		}
	}

	public String toString() {
		return message + "*" + fileUrl + "*" + fileName + "*" + fileSize;
	}

	public int numberOfProperty() {
		int i = 0;
		if (message.length() > 0)
			i++;
		if (fileUrl.length() > 0)
			i++;
		if (fileName.length() > 0)
			i++;
		if (fileSize > 0)
			i++;
		return i;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getFileUrl() {
		return fileUrl;
	}

	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

}
