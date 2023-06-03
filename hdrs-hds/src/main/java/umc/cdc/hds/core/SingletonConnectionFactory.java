/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author brandboat
 */
public class SingletonConnectionFactory {

  private static Connection conn;
  private static final AtomicInteger counter = new AtomicInteger(0);

  private static void createIfNonConnection(Configuration conf) throws IOException {
    conn = ConnectionFactory.createConnection(conf);
  }

  public static HBaseConnection createConnection(Configuration conf) throws IOException {
    if (counter.getAndIncrement() == 0) {
      createIfNonConnection(conf);
    }
    return new HBaseConnection() {
      private boolean isClosed = false;

      @Override
      public Configuration getConfiguration() {
        return conn.getConfiguration();
      }

      @Override
      public Table getTable(TableName tableName) throws IOException {
        return conn.getTable(tableName);
      }

      @Override
      public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        return conn.getTable(tableName, pool);
      }

      @Override
      public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return conn.getBufferedMutator(tableName);
      }

      @Override
      public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        return conn.getBufferedMutator(params);
      }

      @Override
      public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return conn.getRegionLocator(tableName);
      }

      @Override
      public Admin getAdmin() throws IOException {
        return conn.getAdmin();
      }

      @Override
      public void close() throws IOException {
        isClosed = true;
        fakeClose();
      }

      @Override
      public boolean isClosed() {
        return isClosed;
      }

      @Override
      public void abort(String why, Throwable e) {
      }

      @Override
      public boolean isAborted() {
        return false;
      }

      private void fakeClose() throws IOException {
        if (counter.decrementAndGet() == 0) {
          conn.close();
        }
      }

      @Override
      public String getDataLocation(final TableName tableName, final byte[] key) throws IOException {
        String hostName = null;
        if (conn instanceof ClusterConnection) {
          ClusterConnection clusterConn = (ClusterConnection) conn;
          HRegionLocation hrl
              = clusterConn.locateRegion(tableName, key);
          if (hrl != null) {
            hostName = hrl.getServerName().getHostname();
            return hostName;
          }
        } else {
          try (Admin admin = conn.getAdmin()) {
            final ClusterStatus clusterSt = admin.getClusterStatus();
            HRegionInfo regionInfo = null;
            for (HRegionInfo hri : admin.getTableRegions(tableName)) {
              if (Bytes.compareTo(hri.getStartKey(), key) <= 0
                  && (hri.getEndKey().length == 0
                  || Bytes.compareTo(key, hri.getEndKey()) < 0)) {
                regionInfo = hri;
                break;
              }
            }
            if (regionInfo == null) {
              return null;
            }
            for (ServerName serverName : clusterSt.getServers()) {
              ServerLoad load = clusterSt.getLoad(serverName);
              for (RegionLoad rl : load.getRegionsLoad().values()) {
                if (Bytes.compareTo(rl.getName(), regionInfo.getRegionName()) == 0) {
                  return serverName.getHostname();
                }
              }
            }
          }
        }
        return hostName;
      }

        @Override
        public void clearRegionLocationCache() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public TableBuilder getTableBuilder(TableName tn, ExecutorService es) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };
  }
}
