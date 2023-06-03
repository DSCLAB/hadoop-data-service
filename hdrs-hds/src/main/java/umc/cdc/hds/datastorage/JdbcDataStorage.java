/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.LogFactory;
import umc.cdc.hds.core.CsvValueFormat;
import umc.cdc.hds.core.HDS;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.JdbcConnectionManager.JdbcResource;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.exceptions.JdbcDataStorageException;
import umc.cdc.hds.mapping.Mapping;
import umc.cdc.hds.tools.BlockingWritableInputStream;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author jpopaholic
 */
public class JdbcDataStorage implements DataStorage {

  private final JdbcConnectionManager manager;
  private final Configuration config;
  private static final Log LOG = LogFactory.getLog(JdbcDataStorage.class);

  public JdbcDataStorage(Configuration conf) {
    manager = new JdbcConnectionManager(
        conf.getInt(HDSConstants.JDBC_POOL_SIZE, HDSConstants.DEFAULT_JDBC_POOL_SIZE),
        conf.getInt(HDSConstants.JDBC_POOL_CHECKTIME, HDSConstants.DEFAULT_JDBC_POOL_CHECKTIME),
        conf.getLong(HDSConstants.JDBC_CONNECTION_TIMEOUT, HDSConstants.DEFAULT_JDBC_CONNECTION_TIMEOUT)
    );
    config = conf;
  }

  @Override
  public void close() throws IOException {
    manager.close();
  }

  @Override
  public DataStorageOutput create(UriRequest uri) throws IOException {
    throw new JdbcDataStorageException("Not supported yet.");
  }

  @Override
  public DataStorageInput open(UriRequest uri) throws IOException {
    JdbcResource source = manager.getJdbcResource(uri.getQuery().getQueryValue(HDSConstants.JDBC_INFO_QUERY)
        .orElseThrow(() -> new JdbcDataStorageException("Need to write Jdbc connection info!")));
    int queueSize = config.getInt(
        HDSConstants.JDBC_CSV_BUFFER_QUEUE_SIZE,
        HDSConstants.DEFAULT_JDBC_CSV_BUFFER_QUEUE_SIZE);
    int defaultByteSize = config.getInt(HDSConstants.JDBC_CSV_BUFFER_SIZE,
        HDSConstants.DEFAULT_JDBC_CSV_BUFFER_SIZE);
    try {
      LOG.info("########### GO CSVInputStream");
      CSVInputStream in = new CSVInputStream(uri, getResultSet(source, uri), defaultByteSize, queueSize < 3 ? HDSConstants.DEFAULT_JDBC_CSV_BUFFER_QUEUE_SIZE : queueSize);
      return new DataStorageInput() {
        @Override
        public void close() throws IOException {
          in.close();
          source.release();
        }

        @Override
        public int read() throws IOException {
          return in.read();
        }

        @Override
        public int read(byte[] b, int offset, int length) throws IOException {
          return in.read(b, offset, length);
        }

        @Override
        public void recover() {
          //do nothing
        }

        @Override
        public long getSize() {
          return 0;
        }

        @Override
        public String getName() {
          return uri.getQuery().getQueryValue(HDSConstants.JDBC_QUERY_QUERY, "(NONE)");
        }

        @Override
        public String getUri() {
          return uri.toString();
        }

      };

    } catch (Exception ex) {
      LOG.error("jdbcException", ex);
      source.release();
      throw new JdbcDataStorageException(ex);
    }
  }

  @Override
  public CloseableIterator<BatchDeleteRecord> batchDelete(UriRequest uri, boolean enableWildcard) throws IOException {
    throw new JdbcDataStorageException("illegal");
  }

  @Override
  public DataRecord delete(UriRequest uri, boolean isRecursive) throws IOException {
    throw new JdbcDataStorageException("illegal");
  }

  @Override
  public CloseableIterator<DataRecord> list(UriRequest uri, Query optArg) throws IOException {
    throw new JdbcDataStorageException("illegal");
  }

  private ResultSet getResultSet(JdbcResource source, UriRequest uri) throws IOException {
    try {
      ResultSet result = source.getJdbcConnection().createStatement().executeQuery(getSqlQuery(uri));
      return result;
    } catch (SQLException ex) {
      source.release();
      throw new JdbcDataStorageException(ex);
    }
  }

  private String getSqlQuery(UriRequest uri) throws IOException {
    if (uri.getQuery().getQueryValue(HDSConstants.JDBC_QUERY_QUERY).isPresent()) {
      return uri.getQuery().getQueryValue(HDSConstants.JDBC_QUERY_QUERY).get();
    } else {
      return getSqlQueryFromFile(uri);
    }
  }

  private String getSqlQueryFromFile(UriRequest uri) throws IOException {
    try (Mapping mapping = new Mapping(config)) {
      UriRequest queryFile = UriParser.valueOf(uri.getQuery().getQueryValue(HDSConstants.JDBC_QUERY_FILE)
          .orElseThrow(() -> new JdbcDataStorageException("jdbc need fill query or file which context is query")), mapping);
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(HDS.getDataStorage(queryFile, config).open(queryFile)))) {
        StringBuilder context = new StringBuilder();
        String s = reader.readLine();
        while (s != null) {
          context.append(s);
          s = reader.readLine();
        }
        return context.toString();
      }
    }
  }

  @Override
  public long getConnectionNum() {
    return manager.getConnectionNum();
  }

  private class CSVInputStream extends InputStream {

    private final CSVPrinter printer;
    private final BlockingWritableInputStream in;
    private final ResultSetPrinter rsPrinter;

    public CSVInputStream(UriRequest uri, ResultSet result, int defaultByteSize, int queueSize) throws IOException, SQLException {
      String type = uri.getQuery().getQueryValue(HDSConstants.JDBC_CSV_FORMAT_QUERY,
          HDSConstants.JDBC_DEFAULT_CSV_FORMAT);
      CSVFormat format = CSVFormat.DEFAULT;
      try {
        //format=CSVFormat.EXCEL;
        format = CSVFormat.Predefined.valueOf(type).getFormat();
      } catch (IllegalArgumentException ex) {
        //use default format
      }
      in = new BlockingWritableInputStream(defaultByteSize, queueSize);
      printer = new CSVPrinter(in, format);
      rsPrinter = new ResultSetPrinter(result, uri.getQuery().getQueryValueAsBoolean(HDSConstants.JDBC_WITH_HEADER_QUERY,
          HDSConstants.JDBC_DEFAULT_WITH_HEADER));
    }

    @Override
    public int read() throws IOException {
      while (true) {
        try {
          return in.read();
        } catch (EOFException ex) {
          try {
            if (!rsPrinter.print(printer)) {
              in.setNoDataComing();
            }
          } catch (SQLException | InterruptedException ex1) {
            throw new JdbcDataStorageException(ex1);
          }
        }
      }
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      while (true) {
        try {
          return in.read(b, offset, length);
        } catch (EOFException ex) {
          try {
            if (!rsPrinter.print(printer)) {
              in.setNoDataComing();
            }
          } catch (SQLException | InterruptedException ex1) {
            throw new JdbcDataStorageException(ex1);
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      printer.close();
    }

    private class ResultSetPrinter {

      private final ResultSet resultSet;
      private int currentColumn;
      private int columnCount;
      private final boolean printHeader;
      private boolean isPrintHeader;
      private final String[] headers;

      public ResultSetPrinter(ResultSet rs, boolean printHeader) throws SQLException {
        resultSet = rs;
        currentColumn = 1;
        columnCount = 0;
        this.printHeader = printHeader;
        isPrintHeader = false;
        headers = getColumns(rs);
      }

      public boolean print(CSVPrinter printer) throws SQLException, IOException {
        while (true) {
          if (currentColumn <= columnCount) {
            if (printHeader && !isPrintHeader) {
              printer.print(CsvValueFormat.toCsvFormat(headers[currentColumn - 1]));
            } else {
              printer.print(CsvValueFormat.toCsvFormat(resultSet.getObject(currentColumn)));
            }
            if (currentColumn == columnCount) {
              if (printHeader) {
                isPrintHeader = true;
              }
              printer.println();
            }
            currentColumn++;
            return true;
          } else if (printHeader && !isPrintHeader) {
            columnCount = headers.length;
            currentColumn = 1;
          } else if (resultSet.next()) {
            columnCount = resultSet.getMetaData().getColumnCount();
            currentColumn = 1;
          } else {
            return false;
          }
        }
      }

      private String[] getColumns(ResultSet result) throws SQLException {
        ResultSetMetaData meta = result.getMetaData();
        Map<String, Integer> columnNameCount = new HashMap();
        int colsNum = meta.getColumnCount();
        for (int i = 0; i < colsNum; i++) {
          String key = meta.getColumnLabel(i + 1);
          int value = columnNameCount.getOrDefault(key, 0) + 1;
          columnNameCount.put(key, value);
        }
        String[] cols = new String[colsNum];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colsNum; i++) {
          String key = meta.getColumnLabel(i + 1);
          if (columnNameCount.getOrDefault(key, 0) > 1) {
            sb.append(meta.getTableName(i + 1)).append(".");
          }
          sb.append(key);
          cols[i] = sb.toString();
          sb.setLength(0);
        }
        return cols;
      }
    }
  }
}
