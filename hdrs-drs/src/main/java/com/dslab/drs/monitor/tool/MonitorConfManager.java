package com.dslab.drs.monitor.tool;

import com.dslab.drs.simulater.Workload;
import com.dslab.drs.utils.DrsConfiguration;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;

/**
 *
 * @author kh87313
 */
public class MonitorConfManager {

  private final DrsConfiguration monitorConf;
  private final Workload workload;

  public MonitorConfManager(String confFile) throws IOException {
    this.workload = new Workload(confFile);
    this.monitorConf = getMonitorDrsConfiguration(workload);

  }

  public DrsConfiguration getMonitorConf() {
    return this.monitorConf;
  }

  private DrsConfiguration getMonitorDrsConfiguration(Workload workload) throws IOException {

    //將workload設定的值也設定入 DrsConfiguration
    Set<Entry<Object, Object>> allProperties = workload.getAllProperties();
    DrsConfiguration monitorConf = DrsConfiguration.newDrsConfiguration();

    for (Entry<Object, Object> entry : allProperties) {
      monitorConf.set((String) entry.getKey(), (String) entry.getValue());
    }

    DrsConfiguration.replaceHdsConfHost(monitorConf, workload.getStringValue(Workload.HDS_HOST));
    return monitorConf;
  }

  public Level getLogLever() {
    String logLevel = workload.getStringValue(Workload.LOG_LEVEL);
    switch (logLevel) {
      case "warn":
        return Level.WARN;
      case "debug":
        return Level.DEBUG;
      case "error":
        return Level.ERROR;
      case "fatal":
        return Level.FATAL;
      default:
        return Level.INFO;
    }
  }

  public String getRmHttpAddressPort() {
    return workload.getStringValue(Workload.RM_HTTP_ADDRESS);
  }

  public Configuration getDbConf() {
    Configuration dbConf = new Configuration();
    return workload.getDbConf();
  }

}
