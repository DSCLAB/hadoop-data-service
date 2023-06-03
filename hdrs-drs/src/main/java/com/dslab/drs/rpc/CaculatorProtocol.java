package com.dslab.drs.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface CaculatorProtocol extends VersionedProtocol {

  public static final long versionID = 1L;

  public ResponseMessage add(ResponseMessage a, ResponseMessage b);
}
