package com.dslab.drs.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface TaskUmbilicalProtocol extends VersionedProtocol {
	public static final long versionID = 1L;
	public ResponseMessage register(RegisterMessage msg);
	public ResponseMessage done(DoneTaskMessage msg);
	public ResponseMessage fail(FailedTaskMessage msg);
	public ResponseMessage askStatus(AskStatusMessage msg);
}
