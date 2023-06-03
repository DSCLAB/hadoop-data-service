package com.umc.hdrs.dblog;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import com.umc.hdrs.dblog.DbLogger.TableSchema.Column;
import com.umc.hdrs.dblog.LogCell.SqlCell;
import com.umc.hdrs.dblog.LogCell.SqlType;

public final class DbLogger implements Logger {

  private static final Log LOG = LogFactory.getLog(DbLogger.class);
  // thread that keep push data to db
  private final ScheduledThreadPoolExecutor exe = new ScheduledThreadPoolExecutor(1);
  // information about db log table.
  private final TableSchema schema;

  private final String jdbcUrl;

  private final String jdbcDriver;

  private final int logQueueSize;

  private final int retryTimes;

  private final int batchSize;

  private final long logPeriod;

  // check if db log is ready to initialize.
  private AtomicBoolean isDbLogReady = new AtomicBoolean(false);
  // sql for different database.
  @VisibleForTesting
  final DbSql dbSql;
  @VisibleForTesting
  final DbAction dbAction;
  // store log
  @VisibleForTesting
  LogStore store;

//    public DbLogger(TableSchema schema)
//            throws SQLException, ClassNotFoundException, IOException {
//        this(schema, new DbLogConfig(loadCustomHdsConfiguration()));
//    }
  public DbLogger(TableSchema schema, DbLogConfig conf)
      throws ClassNotFoundException, SQLException, IOException {

    this.jdbcUrl = conf.getJdbcUrl();
    this.jdbcDriver = conf.getJdbcDriver();
    this.retryTimes = conf.getRetryTimes();
    this.batchSize = conf.getBatchSize();
    this.logQueueSize = conf.getLogQueueSize();
    this.logPeriod = conf.getPeriod();

    this.schema = schema;

    this.dbSql = SupportedDB.getSupportedDB(conf.getJdbcDbName())
        .orElseThrow(() -> {
          return new IOException("Database not supported.");
        }).getDBSQL();
    this.dbAction = new DbActionImpl(this.dbSql, this.schema, this.jdbcDriver,
        this.jdbcUrl, this.batchSize);
    this.store = new LogStore(logQueueSize);
  }

  @Override
  public void close() {
    exe.shutdownNow();
  }

  @VisibleForTesting
  static final class LogStore {

    private final int size;
    private final LinkedBlockingQueue<LogCell> queue;

    public LogStore() {
      this.size = 1;
      this.queue = new LinkedBlockingQueue<>(size);
    }

    public LogStore(int queueSize) {
      this.size = queueSize;
      this.queue = new LinkedBlockingQueue<>(size);
    }

    public LogCell getLog() {
      return queue.poll();
    }

    public void addLog(LogCell logCell) {
      try {
        queue.add(logCell);
      } catch (IllegalStateException ex) {
        LOG.debug("Queue is full.");
      }
    }

    public boolean isEmpty() {
      return queue.isEmpty();
    }

    public int getSize() {
      return queue.size();
    }

  }

  public static enum SupportedDB {

    MYSQL("mysql", new SqlMySql()),
    ORACLE("oracle", new SqlOracle()),
    SQLSERVER("sqlserver", new SqlMSSqlServer()),
    PHOENIX("phoenix", new SqlPhoenix()),
    POSTGRESQL("postgresql", new SqlPostgres());

    private String dbName;
    private DbSql sql;

    SupportedDB(String dbName, DbSql sql) {
      this.dbName = dbName;
      this.sql = sql;
    }

    public String getDBName() {
      return dbName;
    }

    public DbSql getDBSQL() {
      return sql;
    }

    public static Optional<SupportedDB> getSupportedDB(String dbName) {
      for (SupportedDB db : SupportedDB.values()) {
        if (db.getDBName().equals(dbName.toLowerCase())) {
          return Optional.of(db);
        }
      }
      return Optional.empty();
    }
  }

  public static class DbLogConfig {

    // jdbc driver
    private String jdbcDriver = "";
    // jdbc url
    private String jdbcUrl = "";
    // jdbc db
    private String jdbcDbName = "";
    // upsert frequency
    private long period = 0;
    // retry time
    private int retry = 0;
    // batch insert number
    private int batchSize = 0;
    // log queue size
    private int logQueueSize = 0;

    public DbLogConfig(Configuration conf) {
      this.jdbcDriver = conf.get(Constants.LOGGER_JDBC_DRIVER, "");
      this.jdbcUrl = conf.get(Constants.LOGGER_JDBC_URL, "");
      this.jdbcDbName = conf.get(Constants.LOGGER_JDBC_DB, "");
      this.period = conf.getLong(Constants.LOGGER_RECORD_PERIOD,
          Constants.DEFAULT_LOGGER_RECORD_PERIOD);
      this.retry = conf.getInt(Constants.LOGGER_RETRY_TIMES,
          Constants.DEFAULT_LOGGER_RETRY_TIMES);
      if (retry < 0) {
        throw new RuntimeException("DbLogger retry times should not less than 0.");
      }
      this.batchSize = conf.getInt(Constants.LOGGER_BATCH_SIZE,
          Constants.DEFAULT_LOGGER_BATCH_SIZE);
      this.logQueueSize = conf.getInt(Constants.LOGGER_QUEUE_SIZE,
          Constants.DEFAULT_LOGGER_QUEUE_SIZE);
    }

    public DbLogConfig(String jdbcDriver, String jdbcUrl, String jdbcDbName,
        long period, int retry, int batchSize) {
      this.jdbcDriver = jdbcDriver;
      this.jdbcUrl = jdbcUrl;
      this.jdbcDbName = jdbcDbName;
      this.period = period;
      this.retry = retry;
      if (retry < 0) {
        throw new RuntimeException("DbLogger retry times should not less than 0.");
      }
      this.batchSize = batchSize;
    }

    public long getPeriod() {
      return period;
    }

    public String getJdbcDriver() {
      return jdbcDriver;
    }

    public String getJdbcUrl() {
      return jdbcUrl;
    }

    public String getJdbcDbName() {
      return jdbcDbName;
    }

    public int getRetryTimes() {
      return retry;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public int getLogQueueSize() {
      return logQueueSize;
    }
  }

  public static class TableSchema {

    private final String tableName;
    private final List<Column<String, SqlType>> columns;

    public TableSchema(
        String tableName,
        List<Column<String, SqlType>> columns) {
      this.tableName = tableName;
      this.columns = new LinkedList<>(columns);
    }

    public static class Column<String, SqlType> {

      private final String colName;
      private final SqlType colType;

      public Column(String colName, SqlType colType) {
        this.colName = colName;
        this.colType = colType;
      }

      public String getColName() {
        return colName;
      }

      public SqlType getColType() {
        return colType;
      }

    }

    public String getTableName() {
      return tableName;
    }

    public List<Column<String, SqlType>> getColumns() {
      return columns;
    }
  }

  public static class TableSchemaBuilder {

    private String tableName = "";
    private final List<Column<String, SqlType>> columns = new LinkedList<>();

    public TableSchemaBuilder() {
    }

    public void setTableName(String tn) {
      this.tableName = tn;
    }

    public void addColumn(String colName, SqlType type) {
      columns.add(new Column(colName, type));
    }

    public TableSchema build() throws IOException {
      if (tableName.isEmpty()) {
        throw new IOException("Table name not set.");
      }
      if (columns.isEmpty()) {
        throw new IOException("No column in table.");
      }
      return new TableSchema(tableName, columns);
    }
  }

  public static interface DbSql {

    public String isTableExistSql(TableSchema schema);

    public String createTableSql(TableSchema schema);

    public String dropTableSql(TableSchema schema);

    public String preparedInsertSql(TableSchema schema);

    public String transformSqlType(SqlType type);

    public String transformSqlValue(SqlCell cell);

  }

  @VisibleForTesting
  static interface DbAction {

    public void createTableIfNotExist() throws SQLException;

    public void insert(LogCell cell) throws SQLException;

    public void batchInsert(LogStore store) throws SQLException;

    public void dropTableIfExist() throws SQLException;

    public boolean isTableExist() throws SQLException;
  }

  @VisibleForTesting
  final static class DbActionImpl implements DbAction {

    private final DbSql sql;
    private final TableSchema schema;
    private List<LogCell> tempLogStore = new LinkedList<>();
    // for create uuid
    private final AtomicInteger id = new AtomicInteger(0);
    private final boolean pk;
    private final String jdbcUrl;
    private final int batchSize;

    public DbActionImpl(DbSql sql,
        TableSchema schema,
        String jdbcDriver,
        String jdbcUrl,
        int batchSize)
        throws ClassNotFoundException {
      Class.forName(jdbcDriver);
      this.sql = sql;
      this.schema = schema;
      this.jdbcUrl = jdbcUrl;
      this.batchSize = batchSize;
      this.pk = sql instanceof SqlPhoenix;
    }

    @Override
    public void createTableIfNotExist() throws SQLException {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        if (internalIsTableExist(conn)) {
          LOG.info(schema.getTableName() + " already exists.");
          return;
        }
        LOG.info("Creating Log Table: " + schema.getTableName() + " ...");
        createTable(conn);
        LOG.info("Creating Log Table: " + schema.getTableName() + " Done.");
      }
    }

    @Override
    public void insert(LogCell cell) {

    }

    @Override
    public void batchInsert(LogStore store) throws SQLException {
      if (store.isEmpty()) {
        return;
      }
      prepareLogStoreForBatch(store);
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(sql.preparedInsertSql(schema))) {
          conn.setAutoCommit(false);
          tempLogStore.stream().forEach(log -> {
            fillSqlAndBatch(ps, log);
          });
          executeBatchAndCommit(conn, ps);
        }
      }
    }

    void prepareLogStoreForBatch(LogStore store) {
      if (!tempLogStore.isEmpty()) {
        return;
      }
      for (int i = 0; i < batchSize; i++) {
        LogCell log = store.getLog();
        if (log == null) {
          break;
        }
        tempLogStore.add(log);
      }
    }

    void executeBatchAndCommit(Connection conn, PreparedStatement ps)
        throws SQLException {
      ps.executeBatch();
      conn.commit();
      ps.clearBatch();
      tempLogStore = new LinkedList<>();
    }

    void fillSqlAndBatch(PreparedStatement ps, LogCell cells) {
      try {
        fullfillInsertSql(ps, cells);
      } catch (SQLException ex) {
        LOG.error("fill sql error", ex);
      }
    }

    void fullfillInsertSql(PreparedStatement ps, LogCell log) throws SQLException {
      try {
        int psPos = 0;
        for (Column<String, SqlType> column : schema.getColumns()) {
          SqlCell colVal = log.getLog(column.getColName())
              .orElseThrow(() -> new SQLException("Log "
                  + column.getColName() + " value not found."));
          Object obj = colVal.getCellVal();
          switch (column.getColType()) {
            case BIGINT:
              if (obj == null) {
                ps.setNull(++psPos, java.sql.Types.BIGINT);
              } else {
                ps.setLong(++psPos, (Long) obj);
              }
              break;
            case INTEGER:
              if (obj == null) {
                ps.setNull(++psPos, java.sql.Types.INTEGER);
              } else {
                ps.setInt(++psPos, (Integer) obj);
              }
              break;
            case DATETIME:
              if (obj == null) {
                ps.setNull(++psPos, java.sql.Types.TIMESTAMP);
              } else {
                ps.setTimestamp(++psPos, new Timestamp((Long) obj));
              }
              break;
            case STRING:
              if (obj == null) {
                ps.setNull(++psPos, java.sql.Types.VARCHAR);
              } else {
                ps.setString(++psPos, (String) obj);
              }
              break;
            case TEXT:
              if (obj == null) {
                ps.setNull(++psPos, java.sql.Types.VARCHAR);
              } else {
                ps.setString(++psPos, (String) obj);
              }
              break;
            default:
              throw new SQLException(column.getColType() + " not supported.");
          }
        }
        if (pk) {
          ps.setBytes(++psPos, createUUID());
        }
        ps.addBatch();
      } catch (SQLException ex) {
        ps.clearParameters();
        throw ex;
      }
    }

    byte[] createUUID() {
      String hostName = "localhost";
      try {
        hostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException ex) {
      }
      long timestamp = System.currentTimeMillis();
      int i = id.getAndIncrement();
      return Bytes.add(Bytes.toBytes(hostName),
          Bytes.toBytes(timestamp),
          Bytes.toBytes(i));
    }

    boolean internalIsTableExist(Connection conn) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery(sql.isTableExistSql(schema));
        return true;
      } catch (SQLException ex) {
        return false;
      }
    }

    void createTable(Connection conn) throws SQLException {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(sql.createTableSql(schema));
      }
    }

    @Override
    public void dropTableIfExist() throws SQLException {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        if (!internalIsTableExist(conn)) {
          return;
        }
        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate(sql.dropTableSql(schema));
        }
      }
    }

    @Override
    public boolean isTableExist() throws SQLException {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        return internalIsTableExist(conn);
      }
    }
  }

  public static class SqlMySql implements DbSql {

    public SqlMySql() {
    }

    @Override
    public String isTableExistSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
          .append(schema.getTableName())
          .append(" where 1=0");
      return sb.toString();
    }

    @Override
    public String createTableSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("create table ")
          .append(schema.getTableName())
          .append(" (");
      schema.getColumns().forEach((entry) -> {
        sb.append(entry.getColName())
            .append(" ")
            .append(transformSqlType(entry.getColType()))
            .append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String preparedInsertSql(TableSchema schema) {
      List<Column<String, SqlType>> columns = schema.getColumns();
      StringBuilder sb = new StringBuilder();
      sb.append("insert into ");
      sb.append(schema.getTableName());
      sb.append(" (");
      columns.forEach((col) -> {
        sb.append(col.getColName());
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      sb.append(" values (");
      columns.forEach((col) -> {
        sb.append("?");
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String transformSqlType(SqlType type) {
      switch (type) {
        case STRING:
          return "VARCHAR(255)";
        case BIGINT:
          return "BIGINT";
        case INTEGER:
          return "INTEGER";
        case DATETIME:
          return "DATETIME";
        case TEXT:
          return "TEXT";
        default:
          return "VARCHAR(255)";
      }
    }

    @Override
    public String transformSqlValue(SqlCell cell) {
      switch (cell.getType()) {
        case DATETIME:
          Timestamp ts = new Timestamp((Long) cell.getCellVal());
          return ts.toString();
        default:
          return cell.getCellVal().toString();
      }
    }

    @Override
    public String dropTableSql(TableSchema schema) {
      return "drop table " + schema.getTableName();
    }

  }

  public static class SqlOracle implements DbSql {

    public SqlOracle() {
    }

    @Override
    public String isTableExistSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
          .append(schema.getTableName())
          .append(" where 1=0");
      return sb.toString();
    }

    @Override
    public String createTableSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("create table ")
          .append(schema.getTableName())
          .append(" (");
      schema.getColumns().forEach((entry) -> {
        sb.append(entry.getColName())
            .append(" ")
            .append(transformSqlType(entry.getColType()))
            .append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String preparedInsertSql(TableSchema schema) {
      List<Column<String, SqlType>> columns = schema.getColumns();
      StringBuilder sb = new StringBuilder();
      sb.append("insert into ");
      sb.append(schema.getTableName());
      sb.append(" (");
      columns.forEach((col) -> {
        sb.append(col.getColName());
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      sb.append(" values (");
      columns.forEach((col) -> {
        sb.append("?");
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String transformSqlType(SqlType type) {
      switch (type) {
        case STRING:
          return "VARCHAR(255)";
        case BIGINT:
          return "NUMBER(19)";
        case INTEGER:
          return "INTEGER";
        case DATETIME:
          return "TIMESTAMP";
        case TEXT:
          return "TEXT";
        default:
          return "VARCHAR(255)";
      }
    }

    @Override
    public String transformSqlValue(SqlCell cell) {
      switch (cell.getType()) {
        case DATETIME:
          Timestamp ts = new Timestamp((Long) cell.getCellVal());
          return ts.toString();
        default:
          return cell.getCellVal().toString();
      }
    }

    @Override
    public String dropTableSql(TableSchema schema) {
      return "drop table " + schema.getTableName();
    }

  }

  public static class SqlMSSqlServer implements DbSql {

    public SqlMSSqlServer() {
    }

    @Override
    public String isTableExistSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
          .append(schema.getTableName())
          .append(" where 1=0");
      return sb.toString();
    }

    @Override
    public String createTableSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("create table ")
          .append(schema.getTableName())
          .append(" (");
      schema.getColumns().forEach((entry) -> {
        sb.append(entry.getColName())
            .append(" ")
            .append(transformSqlType(entry.getColType()))
            .append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String preparedInsertSql(TableSchema schema) {
      List<Column<String, SqlType>> columns = schema.getColumns();
      StringBuilder sb = new StringBuilder();
      sb.append("insert into ");
      sb.append(schema.getTableName());
      sb.append(" (");
      columns.forEach((col) -> {
        sb.append(col.getColName());
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      sb.append(" values (");
      columns.forEach((col) -> {
        sb.append("?");
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String transformSqlType(SqlType type) {
      switch (type) {
        case STRING:
          return "VARCHAR(255)";
        case BIGINT:
          return "BIGINT";
        case INTEGER:
          return "INTEGER";
        case DATETIME:
          return "DATETIME";
        case TEXT:
          return "VARCHAR(MAX)";
        default:
          return "VARCHAR(255)";
      }
    }

    @Override
    public String transformSqlValue(SqlCell cell) {
      switch (cell.getType()) {
        case DATETIME:
          Timestamp ts = new Timestamp((Long) cell.getCellVal());
          return ts.toString();
        default:
          return cell.getCellVal().toString();
      }
    }

    @Override
    public String dropTableSql(TableSchema schema) {
      return "drop table " + schema.getTableName();
    }
  }

  public static class SqlPhoenix implements DbSql {

    public SqlPhoenix() {
    }

    @Override
    public String isTableExistSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
          .append(schema.getTableName())
          .append(" where 1=0");
      return sb.toString();
    }

    /**
     * Since phoenix must have primary key, we have to add an additional column
     * for table.
     *
     * @param schema
     * @return
     */
    @Override
    public String createTableSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("create table ")
          .append(schema.getTableName())
          .append(" (");
      schema.getColumns().forEach((entry) -> {
        sb.append(entry.getColName())
            .append(" ")
            .append(transformSqlType(entry.getColType()))
            .append(", ");
      });
      sb.append("id VARBINARY not null primary key)");
      return sb.toString();
    }

    @Override
    public String preparedInsertSql(TableSchema schema) {
      List<Column<String, SqlType>> columns = schema.getColumns();
      StringBuilder sb = new StringBuilder();
      sb.append("upsert into ");
      sb.append(schema.getTableName());
      sb.append(" (");
      columns.forEach(col -> {
        sb.append(col.getColName());
        sb.append(", ");
      });
      sb.append("id");
      sb.append(")");
      sb.append(" values (");
      columns.forEach(col -> {
        sb.append("?");
        sb.append(", ");
      });
      sb.append("?");
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String transformSqlType(SqlType type) {
      switch (type) {
        case STRING:
          return "VARCHAR(255)";
        case BIGINT:
          return "BIGINT";
        case INTEGER:
          return "INTEGER";
        case DATETIME:
          return "TIMESTAMP";
        case TEXT:
          return "VARCHAR";
        default:
          return "VARCHAR(255)";
      }
    }

    @Override
    public String transformSqlValue(SqlCell cell) {
      switch (cell.getType()) {
        case DATETIME:
          Timestamp ts = new Timestamp((Long) cell.getCellVal());
          return ts.toString();
        default:
          return cell.getCellVal().toString();
      }
    }

    @Override
    public String dropTableSql(TableSchema schema) {
      return "drop table " + schema.getTableName();
    }
  }

  public static class SqlPostgres implements DbSql {

    public SqlPostgres() {
    }

    @Override
    public String isTableExistSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
          .append(schema.getTableName())
          .append(" where 1=0");
      return sb.toString();
    }

    @Override
    public String createTableSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("create table ")
          .append(schema.getTableName())
          .append(" (");
      schema.getColumns().forEach((entry) -> {
        sb.append(entry.getColName())
            .append(" ")
            .append(transformSqlType(entry.getColType()))
            .append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String preparedInsertSql(TableSchema schema) {
      List<Column<String, SqlType>> columns = schema.getColumns();
      StringBuilder sb = new StringBuilder();
      sb.append("insert into ");
      sb.append(schema.getTableName());
      sb.append(" (");
      columns.forEach((col) -> {
        sb.append(col.getColName());
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      sb.append(" values (");
      columns.forEach((col) -> {
        sb.append("?");
        sb.append(", ");
      });
      sb.delete(sb.length() - ", ".length(), sb.length());
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String transformSqlType(SqlType type) {
      switch (type) {
        case STRING:
          return "VARCHAR(255)";
        case BIGINT:
          return "BIGINT";
        case INTEGER:
          return "INTEGER";
        case DATETIME:
          return "TIMESTAMP";
        case TEXT:
          return "TEXT";
        default:
          return "VARCHAR(255)";
      }
    }

    @Override
    public String transformSqlValue(SqlCell cell) {
      switch (cell.getType()) {
        case DATETIME:
          Timestamp ts = new Timestamp((Long) cell.getCellVal());
          return ts.toString();
        default:
          return cell.getCellVal().toString();
      }
    }

    @Override
    public String dropTableSql(TableSchema schema) {
      return "drop table " + schema.getTableName();
    }
  }

  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public void addLog(LogCell log) {
    // since phoenix table is hbase-related, we have to wait until
    // regionserver all started. Which means we have to create table
    // when the first request is available.
    if (!isDbLogReady.getAndSet(true)) {
      initializeDbLogger();
    }
    store.addLog(log);
  }

  private synchronized void initializeDbLogger() {
    try {
      dbAction.createTableIfNotExist();
    } catch (SQLException ex) {
      LOG.error("DbLogger create table error.", ex);
    }
    startLog();
  }

  private void startLog() {
    exe.scheduleWithFixedDelay(() -> {
      retryBatch(retryTimes);
    }, 0, logPeriod, TimeUnit.SECONDS);
  }

  private void retryBatch(int retryTimes) {
    int rt = retryTimes;
    if (rt < 0) {
      return;
    }
    try {
      dbAction.batchInsert(store);
    } catch (SQLException ex) {
      LOG.error("sql exception", ex);
      retryBatch(--rt);
    }
  }

}
