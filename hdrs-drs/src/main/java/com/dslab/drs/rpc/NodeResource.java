package com.dslab.drs.rpc;

public class NodeResource {
	private String hostName;
	private int availableMemory;
	private int availableGpuMemory;
	private int availableVirtualCores;

	public NodeResource() {

	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getAvailableMemory() {
		return availableMemory;
	}

	public void setAvailableMemory(int availableMemory) {
		this.availableMemory = availableMemory;
	}

	public int getAvailableGpuMemory() {
		return availableGpuMemory;
	}

	public void setAvailableGpuMemory(int availableGpuMemory) {
		this.availableGpuMemory = availableGpuMemory;
	}

	public int getAvailableVirtualCores() {
		return availableVirtualCores;
	}

	public void setAvailableVirtualCores(int availableVirtualCores) {
		this.availableVirtualCores = availableVirtualCores;
	}

}
