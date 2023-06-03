/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.coprocessor;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;

import static umc.cdc.hds.core.HDSConstants.HDS_CONFIG_LOCATION;
import static umc.cdc.hds.core.HDSConstants.JCIFS_CONFIG_FILENAME;
import umc.cdc.hds.httpserver.HdsHttpServer;
import umc.cdc.hds.tools.ThreadPoolManager;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public class HdsRegionServerObserver implements RegionServerObserver,
        RegionServerCoprocessor {

  private static final Log LOG
      = LogFactory.getLog(HdsRegionServerObserver.class);
  private ShareableObject<HdsHttpServer> httpServer;

  @Override public Optional<RegionServerObserver> getRegionServerObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    Configuration conf = env.getConfiguration();
    loadJcifsConfiguration(conf.get(HDS_CONFIG_LOCATION));
    try {
      httpServer = HdsHttpServer.getInstance(conf);
    } catch (Exception ex) {
      LOG.error(ex);
    }
  }

  @Override
  public void preStopRegionServer(
      ObserverContext<RegionServerCoprocessorEnvironment> env) {
    try {
      ThreadPoolManager.shutdownNowAll();
      httpServer.close();
    } catch (Exception ex) {
      LOG.error(ex);
    }
  }

  private void loadJcifsConfiguration(String confDir) throws IOException {
    Path p = new Path(confDir, JCIFS_CONFIG_FILENAME);
    jcifs.Config.load(new FileInputStream(p.toString()));
  }

}
