/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.task;

import java.util.Map;
import java.util.Optional;
import static umc.cdc.hds.core.HDSConstants.*;
import umc.cdc.hds.parser.RangeValue;
import umc.cdc.hds.task.TaskManager.TaskStateEnum;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author brandboat
 */
public class TaskQuery extends Query {

  private final Optional<String> id;
  private final boolean history;
  private final Optional<String> redirectFrom;
  private final Optional<String> clientName;
  private final Optional<String> serverName;
  private final Optional<String> fromUri;
  private final Optional<String> toUri;
  private final Optional<TaskStateEnum> state;
  private final Optional<Double> progress;
  private final Optional<Long> startTime;
  private final Optional<Long> elaspedTime;
  private final Optional<Long> expectedSize;
  private final Optional<Long> transferredSize;
  private final Optional<RangeValue<Double, Double>> progressRange;
  private final Optional<RangeValue<Long, Long>> startTimeRange;
  private final Optional<RangeValue<Long, Long>> elapsedTimeRange;
  private final Optional<RangeValue<Long, Long>> expectedSizeRange;
  private final Optional<RangeValue<Long, Long>> transferredSizeRange;

  public TaskQuery(Map<String, String> query) {
    super(query);
    id = getQueryValue(TASK_ID);
    history = getQueryValueAsBoolean(TASK_HISTORY, false);
    redirectFrom = getQueryValue(TASK_REDIRECT_FROM);
    serverName = getQueryValue(TASK_SERVER_NAME);
    clientName = getQueryValue(TASK_CLIENT_NAME);
    fromUri = getQueryValue(TASK_FROM);
    toUri = getQueryValue(TASK_TO);
    state = TaskStateEnum.find(getQueryValue(TASK_STATE).orElse(null));
    startTime = getQueryValueAsTime(TASK_START_TIME);
    elaspedTime = getQueryValueAsTime(TASK_ELAPSED);
    expectedSize = getQueryValueAsLong(TASK_EXPECTED_SIZE);
    transferredSize = getQueryValueAsLong(TASK_TRANSFERRED_SIZE);
    progress = getQueryValueAsDouble(TASK_PROGRESS);
    startTimeRange = createStartTimeRange();
    elapsedTimeRange = createElapsedTimeRange();
    expectedSizeRange = createExpectedSizeRange();
    transferredSizeRange = createTransferredSizeRange();
    progressRange = createProgressRange();
  }

  public Optional<String> getId() {
    return id;
  }

  public boolean getEnableHistory() {
    return history;
  }

  public Optional<String> getRedirectedFrom() {
    return redirectFrom;
  }

  public Optional<String> getServerName() {
    return serverName;
  }

  public Optional<String> getClientName() {
    return clientName;
  }

  public Optional<String> getFrom() {
    return fromUri;
  }

  public Optional<String> getTo() {
    return toUri;
  }

  public Optional<TaskStateEnum> getState() {
    return state;
  }

  public Optional<Double> getProgress() {
    return progress;
  }

  public Optional<Long> getStartTime() {
    return startTime;
  }

  public Optional<Long> getElapsed() {
    return elaspedTime;
  }

  public Optional<Long> getExpectedSize() {
    return expectedSize;
  }

  public Optional<Long> getTransferredSize() {
    return transferredSize;
  }

  public Optional<RangeValue<Long, Long>> getStartTimeRange() {
    return startTimeRange;
  }

  public Optional<RangeValue<Long, Long>> getElapsedRange() {
    return elapsedTimeRange;
  }

  public Optional<RangeValue<Long, Long>> getTransferredSizeRange() {
    return transferredSizeRange;
  }

  public Optional<RangeValue<Long, Long>> getExpectedSizeRange() {
    return expectedSizeRange;
  }

  public Optional<RangeValue<Double, Double>> getProgressRange() {
    return progressRange;
  }

  private Optional<RangeValue<Double, Double>> createProgressRange() {
    double minValue = getQueryValueAsDouble(TASK_MIN_PROGRESS, 0.0d),
        maxValue = getQueryValueAsDouble(TASK_MAX_PROGRESS, 1.0d);
    return Optional.of(new RangeValue(minValue, maxValue));
  }

  private Optional<RangeValue<Long, Long>> createStartTimeRange() {
    long minValue = getQueryValueAsTime(TASK_MIN_START_TIME, 0),
        maxValue = getQueryValueAsTime(TASK_MAX_START_TIME, Long.MAX_VALUE);
    return Optional.of(new RangeValue(minValue, maxValue));
  }

  private Optional<RangeValue<Long, Long>> createElapsedTimeRange() {
    long minValue = getQueryValueAsTime(TASK_MIN_ELAPSED, 0),
        maxValue = getQueryValueAsTime(TASK_MAX_ELAPSED, Long.MAX_VALUE);
    return Optional.of(new RangeValue(minValue, maxValue));
  }

  private Optional<RangeValue<Long, Long>> createExpectedSizeRange() {
    long minValue = getQueryValueAsLong(TASK_MIN_EXPECTED_SIZE, 0),
        maxValue = getQueryValueAsLong(TASK_MAX_EXPECTED_SIZE, Long.MAX_VALUE);
    return Optional.of(new RangeValue(minValue, maxValue));
  }

  private Optional<RangeValue<Long, Long>> createTransferredSizeRange() {
    long minValue = getQueryValueAsLong(TASK_MIN_TRANSFERRED_SIZE, 0),
        maxValue = getQueryValueAsLong(TASK_MAX_TRANSFERRED_SIZE, Long.MAX_VALUE);
    return Optional.of(new RangeValue(minValue, maxValue));
  }
}
