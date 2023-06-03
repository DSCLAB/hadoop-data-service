package com.dslab.drs.api;

/**
 *
 * @author caca
 */
public class ApplicationStatus implements java.io.Serializable{

    private String applicationID = "";
    private String AMnode = "";
    private String status = "";
    private String Code = "";
    private String Data = "";
    private String Config = "";
    private String Copyto = "";
    private int fileCount = 0;
    private int progress = 0;
    private long elapsed = 0;
    private int UsedContainers = 0;
    private int UsedVirtualCores = 0;
    private int UsedVirtualMemory = 0;

    public void setApplicationID(String applicationID) {
        this.applicationID = applicationID;
    }

    public void setAMnode(String AMnode) {
        this.AMnode = AMnode;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setCode(String code) {
        this.Code = code;
    }

    public void setData(String data) {
        this.Data = data;
    }

    public void setConfig(String config) {
        this.Config = config;
    }

    public void setCopyto(String copyto) {
        this.Copyto = copyto;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }
 
    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }

    public void setUsedContainers(int UsedContainers) {
        this.UsedContainers = UsedContainers;
    }

    public void setUsedVirtualCores(int UsedVirtualCores) {
        this.UsedVirtualCores = UsedVirtualCores;
    }

    public void setUsedVirtualMemory(int UsedVirtualMemory) {
        this.UsedVirtualMemory = UsedVirtualMemory;
    }

    public String getApplicationID() {
        return this.applicationID;
    }

    public String getAMnode() {
        return this.AMnode;
    }

    public String getStatus() {
        return this.status;
    }

    public String getCode() {
        return this.Code;
    }

    public String getData() {
        return this.Data;
    }

    public String getConfig() {
        return this.Config;
    }

    public String getCopyto() {
        return this.Copyto;
    }

    public int getFileCount() {
        return this.fileCount;
    }

    public int getProgress() {
        return this.progress;
    }

    public long getElapsed() {
        return this.elapsed;
    }
    
    public int getUsedContainers() {
        return this.UsedContainers;
    }
    
    public int getUsedVirtualCores() {
        return this.UsedVirtualCores;
    }
    
    public int getUsedVirtualMemory() {
        return this.UsedVirtualMemory;
    }
    

}
