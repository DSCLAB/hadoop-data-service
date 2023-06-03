package com.dslab.drs.container;

import com.dslab.drs.exception.JRIException;
import com.dslab.drs.exception.RrunTimeException;
import com.dslab.drs.monitor.ProcMemInfo;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author caca
 */
public class ContainerTaskStatus implements java.io.Serializable {

  private String Command;
  private String TaskUrl;
  private String TaskName;
  private String TaskType;
  private String NodeName;
  private boolean Result;
  private Exception ErrorMessage;
  private long TaskRunTime;
  private List<String> OutputFiles;
  private List<String> ConsoleFiles;
  private ProcMemInfo procMemInfo;

  public ContainerTaskStatus(String TaskUrl, String TaskName, String TaskType, String NodeName) {
    this.TaskUrl = TaskUrl;
    this.TaskName = TaskName;
    this.TaskType = TaskType;
    this.NodeName = NodeName;
    this.Result = false;
    this.TaskRunTime = 0;
    OutputFiles = new ArrayList();
    ConsoleFiles = new ArrayList();
  }

  public ContainerTaskStatus() {
    this.TaskName = "none";
    this.TaskType = "none";
    this.NodeName = "none";
    this.Result = false;
    this.TaskRunTime = 0;
    OutputFiles = new ArrayList();
    ConsoleFiles = new ArrayList();
  }

  public void setProcMemInfo(ProcMemInfo pmi) {
    this.procMemInfo = pmi;
  }

  public ProcMemInfo getProcMemInfo() {
    return this.procMemInfo;
  }

  public void setCommand(String command) {
    this.Command = command;
  }

  public void setTaskUrl(String TaskUrl) {
    this.TaskUrl = TaskUrl;
  }

  public void setTaskName(String TaskName) {
    this.TaskName = TaskName;
  }

  public void setErrorMessage(Exception exception) {
    this.ErrorMessage = exception;
  }

  public void setOutputFiles(List<String> outputFiles) {
    this.OutputFiles = outputFiles;
  }

  public void addOutputFiles(List<String> outputFiles) {
    outputFiles.stream().forEach((outputFile) -> {
      this.OutputFiles.add(outputFile);
    });

  }

  public void setConsoleFiles(List<String> ConsoleFiles) {
    this.ConsoleFiles = ConsoleFiles;
  }

  public void setResult(boolean result) {
    this.Result = result;
  }

  public void setTaskRunTime(long timeinsencond) {
    this.TaskRunTime = timeinsencond;
  }

  public String getErrorMessage() {
    String errorMessage = "none";
    if (ErrorMessage == null) {
      return "no Exception";
    } else if (ErrorMessage instanceof JRIException) {
      errorMessage = "JRIException :".concat(ErrorMessage.getMessage());
    } else if (ErrorMessage instanceof RrunTimeException) {
      errorMessage = "RrunTimeException :".concat(ErrorMessage.getMessage());
    }
    return ErrorMessage.toString();
  }

  public Exception getError() {
    return ErrorMessage;
  }

  public String getTaskUrl() {
    return this.TaskUrl;
  }

  public String getTaskName() {
    return this.TaskName;
  }

  public String getTaskType() {
    return this.TaskType;
  }

  public String getNodeName() {
    return this.NodeName;
  }

  public boolean getResult() {
    return this.Result;
  }

  public List<String> getOutputFiles() {
    return this.OutputFiles;
  }

  public List<String> getConsoleFiles() {
    return this.ConsoleFiles;
  }

  public long getTaskRunTime() {
    return this.TaskRunTime;
  }

  public String getCommand() {
    return this.Command;
  }

}
