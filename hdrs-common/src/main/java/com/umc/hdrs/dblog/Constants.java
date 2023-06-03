/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.umc.hdrs.dblog;

/**
 *
 * @author brandboat
 */
public class Constants {

  public static final String LOGGER_JDBC_URL = "hds.logger.jdbc.url";
  public static final String LOGGER_JDBC_DRIVER = "hds.logger.jdbc.driver";
  public static final String LOGGER_JDBC_DB = "hds.logger.jdbc.db";
  public static final String LOGGER_RECORD_PERIOD = "hds.logger.period";
  public static long DEFAULT_LOGGER_RECORD_PERIOD = 2;
  public static final String LOGGER_QUEUE_SIZE = "hds.logger.queue.size";
  public static int DEFAULT_LOGGER_QUEUE_SIZE = 1000;
  public static final String LOGGER_RETRY_TIMES = "hds.logger.retry.times";
  public static int DEFAULT_LOGGER_RETRY_TIMES = 3;
  public static final String LOGGER_BATCH_SIZE = "hds.logger.batch.size";
  public static int DEFAULT_LOGGER_BATCH_SIZE = 30;

}
