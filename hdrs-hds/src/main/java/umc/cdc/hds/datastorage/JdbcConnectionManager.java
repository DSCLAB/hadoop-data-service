/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import umc.cdc.hds.exceptions.JdbcDataStorageException;
import umc.udp.core.framework.RestrictedListMap;
import umc.udp.core.framework.RestrictedListMap.Element;

/**
 *
 * @author jpopaholic
 */
public class JdbcConnectionManager implements Closeable {

  private final RestrictedListMap<String, JdbcConnectionValue> connections;
  private final long timeout;

  public JdbcConnectionManager(int poolSize, int checkTime, long timeout) {
    connections = new RestrictedListMap<>(poolSize, checkTime);
    this.timeout = timeout;
  }

  public JdbcResource getJdbcResource(String connectionInfo) throws IOException {
    return new JdbcResource(connections.get(connectionInfo, () -> {
      try {
        return new JdbcConnectionValue(DriverManager.getConnection(connectionInfo), System.currentTimeMillis(), timeout);
      } catch (SQLException ex) {
        throw new JdbcDataStorageException("connection faild", ex);
      }
    }));
  }

  @Override
  public void close() throws IOException {
    connections.close();
  }

  private static class JdbcConnectionValue implements RestrictedListMap.Value {

    private final Connection connection;
    private final long startTime;
    private final long timeout;

    public JdbcConnectionValue(Connection conn, long st, long timeout) {
      connection = conn;
      startTime = st;
      this.timeout = timeout;
    }

    @Override
    public boolean needClose() {
      return System.currentTimeMillis() - startTime > timeout;
    }

    @Override
    public void afterRelease() {
      //do nothing
    }

    public Connection getConnection() {
      return connection;
    }

    @Override
    public void close() throws IOException {
      try {
        connection.close();
      } catch (SQLException ex) {
        throw new JdbcDataStorageException(ex);
      }
    }
  }

  public class JdbcResource {

    private final Element<String, JdbcConnectionValue> resource;

    public JdbcResource(Element<String, JdbcConnectionValue> e) {
      resource = e;
    }

    public Connection getJdbcConnection() {
      return resource.getValue().getConnection();
    }

    public void release() throws IOException {
      resource.close();
    }
  }

  public long getConnectionNum() {
    return connections.count();
  }
}
