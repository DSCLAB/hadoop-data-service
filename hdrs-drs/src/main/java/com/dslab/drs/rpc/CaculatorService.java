package com.dslab.drs.rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;




public class CaculatorService implements CaculatorProtocol{
	private Configuration conf;
	private Server server;

	public CaculatorService() {
		conf = new Configuration();
	}

	@Override
	public ResponseMessage add(ResponseMessage a, ResponseMessage b) {
		System.out.println("be called");

		return new ResponseMessage(a.message+b.message);
	}

	public void startRpcServer() throws HadoopIllegalArgumentException, IOException{
		server =
		          new RPC.Builder(conf).setProtocol(CaculatorProtocol.class)
		            .setInstance(this).setBindAddress("0.0.0.0")
		            .setPort(1234).setNumHandlers(1).build();

		server.start();
	}

	public static void main(String args[]) throws HadoopIllegalArgumentException, IOException{
		CaculatorService service = new CaculatorService();
		service.startRpcServer();
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return CaculatorProtocol.versionID;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg3) throws IOException {
		// TODO Auto-generated method stub
		return new ProtocolSignature(CaculatorProtocol.versionID, null);
	}
}