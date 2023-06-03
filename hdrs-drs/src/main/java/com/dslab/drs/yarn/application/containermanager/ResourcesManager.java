package com.dslab.drs.yarn.application.containermanager;

/**
 *
 * @author Weli
 */
public interface ResourcesManager {

  void release();

  void request();
}
