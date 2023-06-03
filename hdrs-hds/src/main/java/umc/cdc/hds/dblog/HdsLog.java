/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.dblog;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author brandboat
 */
public class HdsLog {

  public static String START_TIME = "START_TIME";
  public static String HOST = "HOST";
  public static String API = "API";
  public static String INPUT = "INPUT";
  public static String OUTPUT = "OUTPUT";
  public static String DATA_SIZE = "DATA_SIZE";
  public static String REQUEST_INIT = "REQUEST_INIT";
  public static String GET_SOURCE = "GET_SOURCE";
  public static String GET_LOCK = "GET_LOCK";
  public static String ACTION = "ACTION";
  public static String RELEASE_LOCK = "RELEASE_LOCK";
  public static String RELEASE_SOURCE = "RELEASE_SOURCE";
  public static String RESPONSE = "RESPONSE";
  public static String TRANSFERDATA = "TRANSFER_DATA";
  public static String COMMITDATA = "COMMIT_DATA";

  // Since if the api has no stage, it has to be null instead of any value.
  // (like -1)
  private Long requestStartTime = null;
  private String host = null;
  private String api = null;
  private String input = null;
  private String output = null;
  private Long dataSize = null;
  private Long requestInitElapsedTime = null;
  private Long getSourceElapsedTime = null;
  private Long getLockElapsedTime = null;
  private Long actionElapsedTime = null;
  private Long releaseLockElapsedTime = null;
  private Long releaseSourceElapsedTime = null;
  private Long responseElapsedTime = null;
  private Long commitElapsedTime = null;
  private Long transferDataElapsedTime = null;
  private final CountDownLatch latch = new CountDownLatch(1);

  public HdsLog() {
  }

  public void setRequestStartTime(long requestStartTime) {
    this.requestStartTime = requestStartTime;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setApi(String api) {
    this.api = api;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public void setDataSize(Long dataSize) {
    this.dataSize = dataSize;
  }

  public void setRequestInitElapsedTime(Long requestInitElapsedTime) {
    this.requestInitElapsedTime = requestInitElapsedTime;
  }

  public void setGetSourceElapsedTime(Long getSourceElapsedTime) {
    this.getSourceElapsedTime = getSourceElapsedTime;
  }

  public void setGetLockElapsedTime(Long getLockElapsedTime) {
    this.getLockElapsedTime = getLockElapsedTime;
  }

  public void setActionElapsedTime(Long actionElapsedTime) {
    this.actionElapsedTime = actionElapsedTime;
  }

  public void setReleaseLockElapsedTime(Long releaseLockElapsedTime) {
    this.releaseLockElapsedTime = releaseLockElapsedTime;
  }

  public void setReleaseSourceElapsedTime(Long releaseSourceElapsedTime) {
    this.releaseSourceElapsedTime = releaseSourceElapsedTime;
  }

  public void setResponseElapsedTime(long responseElapsedTime) {
    this.responseElapsedTime = responseElapsedTime;
  }

  public void setCommitElapsedTime(long commitElapsedTime) {
    this.commitElapsedTime = commitElapsedTime;
  }

  public void setTransferDataElapsedTime(long transferDataElapsedTime) {
    this.transferDataElapsedTime = transferDataElapsedTime;
  }

  public long getRequestStartTime() {
    return requestStartTime;
  }

  public String getHost() {
    return host;
  }

  public String getApi() {
    return api;
  }

  public String getInput() {
    return input;
  }

  public String getOutput() {
    return output;
  }

  public Optional<Long> getDataSize() {
    return Optional.ofNullable(dataSize);
  }

  public Optional<Long> getRequestInitElapsedTime() {
    return Optional.ofNullable(requestInitElapsedTime);
  }

  public Optional<Long> getGetSourceElapsedTime() {
    return Optional.ofNullable(getSourceElapsedTime);
  }

  public Optional<Long> getGetLockElapsedTime() {
    return Optional.ofNullable(getLockElapsedTime);
  }

  public Optional<Long> getActionElapsedTime() {
    return Optional.ofNullable(actionElapsedTime);
  }

  public Optional<Long> getReleaseLockElapsedTime() {
    return Optional.ofNullable(releaseLockElapsedTime);
  }

  public Optional<Long> getReleaseSourceElapsedTime() {
    return Optional.ofNullable(releaseSourceElapsedTime);
  }

  public Optional<Long> getResponseElapsedTime() {
    return Optional.ofNullable(responseElapsedTime);
  }

  public Optional<Long> getCommitElapsedTime() {
    return Optional.ofNullable(commitElapsedTime);
  }

  public Optional<Long> getTransferDataElapsedTime() {
    return Optional.ofNullable(transferDataElapsedTime);
  }

  public void countDown() {
    latch.countDown();
  }

  public void waitForResponseDone() throws InterruptedException {
    latch.await();
  }
}
