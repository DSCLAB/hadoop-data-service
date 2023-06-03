package com.dslab.drs.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class Client {
	public static void main(String args[]) throws IOException{
		YarnConfiguration conf = new YarnConfiguration();
		CaculatorProtocol proxy;
		InetSocketAddress addr = new InetSocketAddress("192.168.103.135",1234);
		proxy = RPC.getProxy(CaculatorProtocol.class, CaculatorProtocol.versionID, addr, conf);
		ResponseMessage a =new ResponseMessage("a");
		ResponseMessage b =new ResponseMessage("B");
		Object o = proxy.add(a, b);

		System.out.println(o);


	}
}
