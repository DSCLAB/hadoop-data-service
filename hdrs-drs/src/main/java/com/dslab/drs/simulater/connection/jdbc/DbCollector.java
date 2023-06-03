package com.dslab.drs.simulater.connection.jdbc;

import com.dslab.drs.drslog.DrsPhaseLogHelper;
import com.dslab.drs.drslog.DrsResourceLogHelper;
import com.dslab.drs.monitor.MonitorConstant;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.umc.hdrs.dblog.DbLogger.DbLogConfig;
import com.umc.hdrs.dblog.DbLogger.TableSchema;
import com.umc.hdrs.dblog.LogCell.SqlType;
import static com.umc.hdrs.dblog.LogCell.SqlType.DATETIME;

public class DbCollector {

  private static final Log LOG = LogFactory.getLog(DbCollector.class);

  // INSTANCE
  private volatile static DbCollector INSTANCE;

  private final String jdbcUrl;
  private final String jdbcDriver;

  // sql for different database.
  final DbSql dbSql;
  final DbAction dbAction;

  private final TableSchema phaseTableSchema;
  private final TableSchema resourceTableSchema;

  private DbCollector(DbLogConfig conf) throws IOException, ClassNotFoundException {
    this.jdbcUrl = conf.getJdbcUrl();
    this.jdbcDriver = conf.getJdbcDriver();

    this.dbSql = SupportedDB.getSupportedDB(conf.getJdbcDbName())
            .orElseThrow(() -> {
              return new IOException("Database not supported.");
            }).getDBSQL();
    this.dbAction = new DbActionImpl(this.dbSql, this.jdbcDriver, this.jdbcUrl);

    this.phaseTableSchema = DrsPhaseLogHelper.getInfo();
    this.resourceTableSchema = DrsResourceLogHelper.getInfo();
  }

  public static DbCollector getDbCollector(DbLogConfig conf) throws IOException {
    if (INSTANCE == null) {
      synchronized (DbCollector.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new DbCollector(conf);
          } catch (Exception ex) {
            throw new IOException(ex);
          }
        }
      }
    }
    return INSTANCE;
  }

  public static DbCollector getDbCollector(Configuration conf) throws IOException {
    return getDbCollector(new DbLogConfig(conf));
  }

  public Optional<Long> getRequestMemory(String applicationId) {
    return dbAction.getRequestMemory(applicationId);
  }

  public Optional<Long> getRequestVcores(String applicationId) {
    return dbAction.getRequestVcores(applicationId);
  }

  public Optional<Double> getAvgPhaseRuntime(String applicationId, String logType) {
    return dbAction.getAvgPhaseRuntime(applicationId, logType);
  }

  public Optional<Double> getAvgPhaseMemoryUsage(String applicationId, String logType) {
    return dbAction.getAvgPhaseMemoryUsage(applicationId, logType);
  }

  public Optional<Double> getAvgPhaseCpuRate(String applicationId, String logType) {
    return dbAction.getAvgPhaseCpuRate(applicationId, logType);
  }

  public Set<String> selectContainerList(String applicationId) throws SQLException {
    LOG.debug("Select contianer list of " + applicationId);
    return dbAction.selectContainerList(phaseTableSchema, applicationId);
  }

  public Optional<DbContainerResult> computeContainerResourceUsage(String containerId, Optional<Timestamp> lastQueryTime) throws SQLException {
    return dbAction.computeContainerResourceUsage(resourceTableSchema, containerId, lastQueryTime);
  }

  public Optional<DbContainerResult> computeContainerResourceUsageOnSql(String containerId) throws SQLException {
    return dbAction.computeContainerResourceUsageOnSql(containerId);
  }

  public boolean isAllTableExist() throws SQLException {
    return dbAction.isTableExist(phaseTableSchema) && dbAction.isTableExist(resourceTableSchema);
  }

  public boolean isApplicationIdExistOnTable( String appId) {
    return dbAction.isApplicationIdExistOnTable(phaseTableSchema, appId);
  }

  public Optional<ApplicationResourceInfos> getApplicationResourceInfos(String applicationId) {
    return dbAction.getApplicationResourceInfos(applicationId);
  }

  public static enum SupportedDB {

    MYSQL("mysql", new SqlMySql()),
    ORACLE("oracle", new SqlOracle()),
    SQLSERVER("sqlserver", new SqlMSSqlServer()),
    PHOENIX("phoenix", new SqlPhoenix()),
    POSTGRESQL("postgresql", new SqlPostgres());

    private final String dbName;
    private final DbSql sql;

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

  public static interface DbSql {

    public String getSelectAllSql(TableSchema schema);

    public String getSelectLatestTimeSql(TableSchema schema);

    public String getSelectEarliestTimeSql(TableSchema schema);

    //讀取DrsPhase的所有Phase狀態是 "AM_PREPARE", schema 確定是 DrsPhaseLogHelper.getInfo
    public String getSelectContainerListSql(TableSchema phaseTableSchema, String applicationId);

    public String getSelectContainerResourceUsageSqlSinceDateSql(TableSchema schema, String containerId, Timestamp lastQueryTime);

    //讀取Resource table的資源使用狀態, schema 確定是 DrsResourceLogHelper.getInfo
    public String getSelectContainerResourceUsageSql(TableSchema resourceTableSchema, String containerId);

    public String isTableExistSql(TableSchema schema);

    public String isApplicationIdExistOnTableSql(TableSchema schema, String appId);

    public String transformSqlType(SqlType type);

    public String getSelectWhereStringSql(TableSchema schema, List<String> selectList, Map<String, String> whereList);

    public String getPhaseAvgSql(TableSchema schema, String logType, String applicationId);

    public String getRequestMemorySql(TableSchema schema, String applicationId);

    public String getRequestVcoresSql(TableSchema schema, String applicationId);

    public String getAvgCpuUsageSql(TableSchema schema, String containerId);

    public String getMaxCpuUsageSql(TableSchema schema, String containerId);

    public String getAvgJvmMemSql(TableSchema schema, String containerId);

    public String getMaxJvmMemSql(TableSchema schema, String containerId);

    public String getAvgProcMemSql(TableSchema schema, String containerId);

    public String getMaxProcMemSql(TableSchema schema, String containerId);

    public String getMaxRMemSql(TableSchema schema, String containerId);

    public String getAvgPhaseRuntimeSql(TableSchema schema, String applicationId, String logType);

    public String getAvgPhaseMemoryUsageSql(TableSchema schema, String applicationId, String logType);

    public String getAvgPhaseCpuRateSql(TableSchema schema, String applicationId, String logType);

    public String getApplicationResourceInfosSql(TableSchema schema, String applicationId);

  }

  static interface DbAction {

    public void selectAll(TableSchema schema) throws SQLException;

    public Optional<Timestamp> getLatestDate(TableSchema schema);

    public Optional<Timestamp> getEarliestDate(TableSchema schema);

    public Set<String> selectContainerList(TableSchema schema, String containerId) throws SQLException;

    public Optional<DbContainerResult> computeContainerResourceUsage(TableSchema resourceTableSchema, String containerId, Optional<Timestamp> lastQueryTime) throws SQLException;

    public Optional<DbContainerResult> computeContainerResourceUsageOnSql(String containerId) throws SQLException;

    public boolean isTableExist(TableSchema schema) throws SQLException;

    public boolean isApplicationIdExistOnTable(TableSchema schema, String appId);

    public Optional<Double> getPhaseAvg(TableSchema schema, String logType, String applicationId);

    public Optional<Long> getRequestMemory(String applicationId);

    public Optional<Long> getRequestVcores(String applicationId);

    public Optional<Double> getAvgCpuUsage(String containerId);

    public Optional<Long> getMaxCpuUsage(String containerId);

    public Optional<Double> getAvgJvmMem(String containerId);

    public Optional<Long> getMaxJvmMem(String containerId);

    public Optional<Double> getAvgProcMem(String containerId);

    public Optional<Long> getMaxProcMem(String containerId);

    public Optional<Long> getMaxRMem(String containerId);

    public Optional<Double> getAvgPhaseRuntime(String applicationId, String logType);

    public Optional<Double> getAvgPhaseMemoryUsage(String applicationId, String logType);

    public Optional<ApplicationResourceInfos> getApplicationResourceInfos(String applicationId);

    public Optional<Double> getAvgPhaseCpuRate(String applicationId, String logType);

  }

  final static class DbActionImpl implements DbAction {

    private final DbSql sql;
    private final String jdbcUrl;

    private final TableSchema phaseTableSchema;
    private final TableSchema resourceTableSchema;

    public DbActionImpl(DbSql sql,
            String jdbcDriver,
            String jdbcUrl)
            throws ClassNotFoundException, IOException {
      Class.forName(jdbcDriver);
      this.sql = sql;
      this.phaseTableSchema = DrsPhaseLogHelper.getInfo();
      this.resourceTableSchema = DrsResourceLogHelper.getInfo();
      this.jdbcUrl = jdbcUrl;
    }

    boolean internalIsTableExist(Connection conn, TableSchema schema) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery(sql.isTableExistSql(schema));
        return true;
      } catch (SQLException ex) {
        return false;
      }
    }

    @Override
    public boolean isTableExist(TableSchema schema) throws SQLException {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        boolean isExist = internalIsTableExist(conn, schema);
        LOG.debug("Table " + schema.getTableName() + "Exist:" + isExist);
        return isExist;
      }
    }

    @Override
    public boolean isApplicationIdExistOnTable(TableSchema schema, String appId) {
      return isExistResult(sql.isApplicationIdExistOnTableSql(schema, appId));
    }

    @Override
    public void selectAll(TableSchema schema) throws SQLException {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(sql.getSelectAllSql(schema))) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:{}" + sql.getSelectAllSql(schema));
          } else {
            do {
              LOG.info(resultSet.toString());
            } while (resultSet.next());
          }
        }
      }
    }

    @Override
    public Set<String> selectContainerList(TableSchema schema, String applicationId) throws SQLException {
      LOG.debug("Select ContainerList where application = " + applicationId);
      Set<String> containerList = new HashSet<>();
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(sql.getSelectContainerListSql(phaseTableSchema, applicationId))) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + sql.getSelectContainerListSql(phaseTableSchema, applicationId));
          } else {
            while (resultSet.next()) {
              String containerId = resultSet.getString(DrsPhaseLogHelper.CONTAINER_ID);
              if (!containerList.contains(containerId)) {
                containerList.add(containerId);
                LOG.debug("find containerId = " + containerId + " from " + applicationId);
              }
            }
          }
        }
      }
      return containerList;
    }

    @Override
    public Optional<DbContainerResult> computeContainerResourceUsage(TableSchema resourceTableSchema, String containerId, Optional<Timestamp> lastQueryTime) throws SQLException {
//      LOG.debug("SQL:" + sql.getSelectContainerResourceUsageSql(resourceTableSchema, containerId));

      String selectContainerResourceUsageSql = lastQueryTime.isPresent()
              ? sql.getSelectContainerResourceUsageSqlSinceDateSql(resourceTableSchema, containerId, lastQueryTime.get())
              : sql.getSelectContainerResourceUsageSql(resourceTableSchema, containerId);
      LOG.debug("Compute resource Usage of " + containerId + ":" + selectContainerResourceUsageSql);
//      LOG.debug(Date.);

      return computeResourceUsage(containerId, selectContainerResourceUsageSql);
    }

    @Override
    public Optional<DbContainerResult> computeContainerResourceUsageOnSql(String containerId) throws SQLException {

      LOG.debug("Compute resource Usage of " + containerId + " on sql.");

      Optional<Double> avgCpuUsage = this.getAvgCpuUsage(containerId);
      Optional<Double> avgJvmUsage = this.getAvgJvmMem(containerId);
      Optional<Double> avgProcUsage = this.getAvgProcMem(containerId);

      Optional<Long> maxCpuUsage = this.getMaxCpuUsage(containerId);
      Optional<Long> maxJvmMem = this.getMaxJvmMem(containerId);
      Optional<Long> maxProcMem = this.getMaxProcMem(containerId);

      Optional<Long> maxRMem = this.getMaxRMem(containerId);

      LOG.debug("containerId:" + containerId);
      LOG.debug("avgCpuUsage:" + avgCpuUsage);
      LOG.debug("avgJvmUsage:" + avgJvmUsage);
      LOG.debug("avgProcUsage:" + avgProcUsage);
      LOG.debug("maxCpuUsage:" + maxCpuUsage);
      LOG.debug("maxJvmMem:" + maxJvmMem);
      LOG.debug("maxProcMem:" + maxProcMem);
      LOG.debug("maxRMem:" + maxRMem);

      if (!avgCpuUsage.isPresent()
              || !avgJvmUsage.isPresent()
              || !avgProcUsage.isPresent()
              || !maxCpuUsage.isPresent()
              || !maxJvmMem.isPresent()
              || !maxProcMem.isPresent()
              || !maxRMem.isPresent()) {
        return Optional.empty();
      } else {
        return Optional.of(new DbContainerResult(containerId,
                new Timestamp(0),
                avgCpuUsage.get().longValue(),
                avgJvmUsage.get().longValue(),
                avgProcUsage.get().longValue(),
                maxCpuUsage.get(),
                maxJvmMem.get(),
                maxProcMem.get(),
                maxRMem.get()));
      }
    }

    private Optional<DbContainerResult> computeResourceUsage(String containerId, String computeResourceSql) throws SQLException {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(computeResourceSql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:{}" + computeResourceSql);
          } else {

            Timestamp lastestTime = new Timestamp(0);

            int count = 0;
            long totalCpuUsage = 0;
            long totalJvmMem = 0;
            long totalProcMem = 0;
            long maxCpuUsage = 0;
            long maxJvmMem = 0;
            long maxProcMem = 0;
            long maxRMem = 0;
            long rMem;

            while (resultSet.next()) {
              if (resultSet.getTimestamp(DrsResourceLogHelper.TIME) == null
                      || resultSet.getBigDecimal(DrsResourceLogHelper.CPU_USAGE) == null
                      || resultSet.getBigDecimal(DrsResourceLogHelper.JVM_MEM_USAGE) == null
                      || resultSet.getBigDecimal(DrsResourceLogHelper.PROC_MEM_USAGE) == null) {
//                LOG.debug("Compute usage encounter null value:" + resultSet.toString());
                continue;
              }

              Timestamp timestamp = resultSet.getTimestamp(DrsResourceLogHelper.TIME);

              long cpuUsage = resultSet.getBigDecimal(DrsResourceLogHelper.CPU_USAGE).longValue();
              long jvmMem = resultSet.getBigDecimal(DrsResourceLogHelper.JVM_MEM_USAGE).longValue();
              long procMem = resultSet.getBigDecimal(DrsResourceLogHelper.PROC_MEM_USAGE).longValue();

//              LOG.debug(time.toString() + " ," + cpuUsage + " ," + jvmMem + " ," + procMem);
              count++;
              lastestTime = lastestTime.after(timestamp) ? lastestTime : timestamp;
              totalCpuUsage = totalCpuUsage + cpuUsage;
              totalJvmMem = totalJvmMem + jvmMem;
              totalProcMem = totalProcMem + procMem;
              rMem = procMem - jvmMem;

              maxCpuUsage = maxCpuUsage > cpuUsage ? maxCpuUsage : cpuUsage;
              maxJvmMem = maxJvmMem > jvmMem ? maxJvmMem : jvmMem;
              maxProcMem = maxProcMem > procMem ? maxProcMem : procMem;
              maxRMem = maxRMem > rMem ? maxRMem : rMem;

//              LOG.debug("containerId " + containerId + " ,cpuUsage= " + cpuUsage + " ,jvmMem= " + jvmMem + " ,procMem= " + procMem);
            } //end while

            if (count == 0) {
              LOG.info("Query no data:" + computeResourceSql);
            } else {
              LOG.debug("count:" + count + ",avgCpuUsage" + totalCpuUsage + ",avgCpuUsage" + totalJvmMem + ",avgCpuUsage" + totalProcMem);
              long avgCpuUsage = totalCpuUsage / count;
              long avgJvmMem = totalJvmMem / count;
              long avgProcMem = totalProcMem / count;

              LOG.debug(containerId + "'s ResourceUsage :");
              LOG.debug("Lastest time :" + lastestTime);
              LOG.debug("avg cpu used rate : " + avgCpuUsage);
              LOG.debug("Max cpu used : " + maxCpuUsage);
              LOG.debug("avg jvm used rate : " + avgJvmMem);
              LOG.debug("Max jvm used :" + maxJvmMem);
              LOG.debug("avg proc mem used rate :" + avgProcMem);
              LOG.debug("Max proc mem used :" + maxProcMem);
              LOG.debug("Max R mem used :" + maxRMem);
              return Optional.of(new DbContainerResult(containerId,
                      lastestTime,
                      avgCpuUsage,
                      avgJvmMem,
                      avgProcMem,
                      maxCpuUsage,
                      maxJvmMem,
                      maxProcMem,
                      maxRMem));
            }
          }
        }
        return Optional.empty();
      }
    }

    @Override
    public Optional<Timestamp> getLatestDate(TableSchema schema) {
      LOG.debug("Get lastest date from" + schema.getTableName());
      String getLastestDateSql = sql.getSelectLatestTimeSql(schema);
      return getDateValue(schema, getLastestDateSql);
    }

    @Override
    public Optional<Timestamp> getEarliestDate(TableSchema schema) {
      LOG.debug("Get lastest date from" + schema.getTableName());
      String getEarliestDate = sql.getSelectEarliestTimeSql(schema);
      return getDateValue(schema, getEarliestDate);
    }

    private Optional<Timestamp> getDateValue(TableSchema schema, String querySql) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + querySql);
          } else if (resultSet.next()) {
            return Optional.of(resultSet.getTimestamp(DrsPhaseLogHelper.TIME));
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return Optional.empty();
    }

    @Override
    public Optional<Double> getPhaseAvg(TableSchema schema, String logType, String applicationId) {
      LOG.debug("Get " + logType + " avg from " + schema.getTableName());
      String getPhaseAvgSql = sql.getPhaseAvgSql(schema, logType, applicationId);
      return getDoubleValue(getPhaseAvgSql, DrsPhaseLogHelper.RUN_TIME);
    }

    private boolean isExistResult(String querySql) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            return false;
          } else if (resultSet.next()) {
            return true;
          } else {
            return false;
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return false;
    }

    private Optional<Double> getDoubleValue(String querySql, String columnName) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + querySql);
          } else if (resultSet.next()) {
            return Optional.of(resultSet.getDouble(columnName));
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return Optional.empty();
    }

    private Optional<Double> getDoubleValue(String querySql, int columnIndex) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + querySql);
          } else if (resultSet.next()) {
            return Optional.of(resultSet.getDouble(columnIndex));
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return Optional.empty();
    }

    @Override
    public Optional<Long> getRequestMemory(String applicationId) {
      String requestMemorySql = sql.getRequestMemorySql(phaseTableSchema, applicationId);
      LOG.debug("Get request memory from " + applicationId + "," + phaseTableSchema.getTableName());
      return getLongValue(requestMemorySql, DrsPhaseLogHelper.RUN_TIME);
    }

    @Override
    public Optional<Long> getRequestVcores(String applicationId) {

      String requestVcoresSql = sql.getRequestVcoresSql(phaseTableSchema, applicationId);

      LOG.debug("Get request vCores from " + applicationId + "," + phaseTableSchema.getTableName());
      return getLongValue(requestVcoresSql, DrsPhaseLogHelper.RUN_TIME);
    }

    @Override
    public Optional<Double> getAvgCpuUsage(String containerId) {
      LOG.debug("Get avg CPU Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getAvgCpuUsageSql = sql.getAvgCpuUsageSql(resourceTableSchema, containerId);
      return getDoubleValue(getAvgCpuUsageSql, 1);
    }

    @Override
    public Optional<Double> getAvgJvmMem(String containerId) {
      LOG.debug("Get avg Jvm Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getAvgJvmUsageSql = sql.getAvgJvmMemSql(resourceTableSchema, containerId);
      return getDoubleValue(getAvgJvmUsageSql, 1);
    }

    @Override
    public Optional<Double> getAvgProcMem(String containerId) {
      LOG.debug("Get avg proc mem Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getAvgProcUsageSql = sql.getAvgProcMemSql(resourceTableSchema, containerId);
      return getDoubleValue(getAvgProcUsageSql, 1);
    }

    @Override
    public Optional<Long> getMaxCpuUsage(String containerId) {
      LOG.debug("Get max CPU Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getAvgCpuUsageSql = sql.getMaxCpuUsageSql(resourceTableSchema, containerId);
      return getLongValue(getAvgCpuUsageSql, 1);
    }

    @Override
    public Optional<Long> getMaxJvmMem(String containerId) {
      LOG.debug("Get max Jvm Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getMaxJvmUsageSql = sql.getMaxJvmMemSql(resourceTableSchema, containerId);
      return getLongValue(getMaxJvmUsageSql, 1);
    }

    @Override
    public Optional<Long> getMaxProcMem(String containerId) {
      LOG.debug("Get max Proc Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getMaxProcUsageSql = sql.getMaxProcMemSql(resourceTableSchema, containerId);
      return getLongValue(getMaxProcUsageSql, 1);
    }

    @Override
    public Optional<Long> getMaxRMem(String containerId) {
      LOG.debug("Get max R Usage from " + containerId + "," + resourceTableSchema.getTableName());
      String getMaxRUsageSql = sql.getMaxRMemSql(resourceTableSchema, containerId);
      return getLongValue(getMaxRUsageSql, 1);
    }

    @Override
    public Optional<Double> getAvgPhaseCpuRate(String applicationId, String logType) {
      LOG.debug("Get avg " + logType + " Usage from " + applicationId);
      String getAvgPhaseCpuRateSql = sql.getAvgPhaseCpuRateSql(resourceTableSchema, applicationId, logType);
      return getDoubleValue(getAvgPhaseCpuRateSql, 1);
    }

    @Override
    public Optional<Double> getAvgPhaseMemoryUsage(String applicationId, String logType) {
      LOG.debug("Get avg " + logType + " Usage from " + applicationId);
      String getAvgPhaseMemoryUsageSql = sql.getAvgPhaseMemoryUsageSql(resourceTableSchema, applicationId, logType);
      return getDoubleValue(getAvgPhaseMemoryUsageSql, 1);
    }

    @Override
    public Optional<Double> getAvgPhaseRuntime(String applicationId, String logType) {
      LOG.debug("Get avg " + logType + " Usage from " + applicationId);
      String getAvgPhaseRuntimeSql = sql.getAvgPhaseRuntimeSql(phaseTableSchema, applicationId, logType);
      return getDoubleValue(getAvgPhaseRuntimeSql, 1);
    }

//    public Optional<Double> getAvgPhaseRuntime(String applicationId, String logType);
//    public Optional<Double> getAvgPhaseMemoryUsage(String applicationId, String logType);
//    public Optional<Double> getAvgPhaseCpuRate(String applicationId, String logType);
    private Optional<Long> getLongValue(String querySql, String columnName) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + querySql);
          } else if (resultSet.next()) {
            return Optional.of(resultSet.getLong(columnName));
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return Optional.empty();
    }

    private Optional<Long> getLongValue(String querySql, int columnIndex) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + querySql);
          } else if (resultSet.next()) {
            return Optional.of(resultSet.getLong(columnIndex));
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return Optional.empty();
    }

    @Override
    public Optional<ApplicationResourceInfos> getApplicationResourceInfos(String applicationId) {
      LOG.debug("Get application resource infos from " + applicationId);
      String querySql = sql.getApplicationResourceInfosSql(resourceTableSchema, applicationId);
      LOG.debug("querySql : " + querySql);
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
          ResultSet resultSet = ps.executeQuery();
          if (!resultSet.isBeforeFirst()) {
            LOG.info("Query no data:" + querySql);
          } else if (resultSet.next()) {
            ApplicationResourceInfos appResourceInfos = new ApplicationResourceInfos();
            appResourceInfos.setAppId(applicationId);
//            try {
            appResourceInfos.setAvgCpu((long) resultSet.getDouble("AVG_CPU_USAGE"));
            appResourceInfos.setMaxCpu((long) resultSet.getDouble("MAX_CPU_USAGE"));
            appResourceInfos.setAvgProcMem((long) resultSet.getDouble("AVG_PROC_MEM"));
            appResourceInfos.setMaxProcMem((long) resultSet.getDouble("MAX_PROC_MEM"));
            appResourceInfos.setAvgJvmMem((long) resultSet.getDouble("AVG_JVM_MEM"));
            appResourceInfos.setMaxJvmMem((long) resultSet.getDouble("MAX_JVM_MEM"));
            appResourceInfos.setAvgRMem((long) resultSet.getDouble("AVG_R_MEM"));
            appResourceInfos.setMaxRMem((long) resultSet.getDouble("MAX_R_MEM"));

            return Optional.of(appResourceInfos);
//            } catch (java.lang.ArrayIndexOutOfBoundsException ex) {
//              ex.printStackTrace();
//              return Optional.empty();
//            }
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return Optional.empty();

    }

  }

  public static class SqlMySql extends NormalSql {

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
    public String getSelectContainerResourceUsageSqlSinceDateSql(TableSchema schema, String containerId, Timestamp lastQueryTime) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }

  public static class SqlOracle extends NormalSql {

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
    public String getSelectContainerResourceUsageSqlSinceDateSql(TableSchema schema, String containerId, Timestamp lastQueryTime) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }

  public static class SqlMSSqlServer extends NormalSql {

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
    public String getSelectContainerResourceUsageSqlSinceDateSql(TableSchema schema, String containerId, Timestamp lastQueryTime) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }

  public static class SqlPhoenix extends NormalSql {

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

//    @Override
//    public String getSelectAllBetweenDateSql(TableSchema resourceTableSchema, Date from, Date to) {
//      return "select * from " + resourceTableSchema.getTableName() + "  where TIME >= TO_TIMESTAMP('" + from.toString() + "') AND TIME <= TO_TIMESTAMP('" + to.toString() + "')";
//    }
//
//    @Override
//    public String getSelectAllSinceDateSql(TableSchema resourceTableSchema, Date from) {
//      return "select * from " + resourceTableSchema.getTableName() + "  where TIME >= TO_TIMESTAMP('" + from.toString() + "')";
//    }
    @Override
    public String getSelectContainerResourceUsageSqlSinceDateSql(TableSchema schema, String containerId, Timestamp lastQueryTime) {
      return "select * from " + schema.getTableName() + " where " + DrsResourceLogHelper.CONTAINER_ID + "='" + containerId + "'" + " AND TIME > TO_TIMESTAMP('" + lastQueryTime.toString() + "')";
    }
  }

  public static class SqlPostgres extends NormalSql {

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
    public String getSelectContainerResourceUsageSqlSinceDateSql(TableSchema schema, String containerId, Timestamp lastQueryTime) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }

  public static abstract class NormalSql implements DbSql {

    @Override
    public final String getSelectAllSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
              .append(schema.getTableName());
      return sb.toString();
    }

    @Override
    public final String getSelectLatestTimeSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select MAX(")
              .append(DrsPhaseLogHelper.TIME)
              .append(") from ")
              .append(schema.getTableName());
      return sb.toString();
    }

    @Override
    public final String getSelectEarliestTimeSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select MIN(")
              .append(DrsPhaseLogHelper.TIME)
              .append(") from ")
              .append(schema.getTableName());
      return sb.toString();
    }

    @Override
    public final String getSelectContainerListSql(TableSchema schema, String applicationId) {
      return "select * from " + schema.getTableName() + " where " + DrsPhaseLogHelper.APPLICATION_NAME + "='" + applicationId + "'";
    }

    @Override
    public final String getSelectContainerResourceUsageSql(TableSchema schema, String containerId) {
      return "select * from " + schema.getTableName() + " where " + DrsResourceLogHelper.CONTAINER_ID + "='" + containerId + "'";
    }

    @Override
    public final String isTableExistSql(TableSchema schema) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
              .append(schema.getTableName())
              .append(" where 1=0");
      return sb.toString();
    }

    @Override
    public final String isApplicationIdExistOnTableSql(TableSchema schema, String appId) {
      StringBuilder sb = new StringBuilder();
      sb.append("select * from ")
              .append(schema.getTableName())
              .append(" where " + DrsResourceLogHelper.APPLICATION_NAME + "=\'" + appId + "\'")
              .append(" limit 1");
      return sb.toString();
    }

    @Override
    public final String getSelectWhereStringSql(TableSchema schema, List<String> selectList, Map<String, String> whereList) {
      StringBuilder sb = new StringBuilder("select ");
      if (selectList.size() > 0) {
        sb.append(" " + selectList.get(0) + " ");
        for (int i = 1; i < selectList.size(); i++) {
          sb.append(",").append(selectList.get(i));
        }
      } else {
        sb.append("*");
      }

      sb.append(" from ");
      sb.append(schema.getTableName());

      if (whereList.size() > 0) {
        sb.append(" where ");
        sb.append(whereList.get(0));
        for (int i = 1; i < whereList.size(); i++) {
          sb.append(" AND ").append(whereList.get(i));
        }
      }

      return sb.toString();
    }

    @Override
    public String getPhaseAvgSql(TableSchema schema, String logType, String applicationId) {
//      "select AVG(RUN_TIME) from DRS_PHASE where LOG_TYPE = R_COMPUTE AND "

      return "select AVG(" + DrsPhaseLogHelper.RUN_TIME + ") from " + schema.getTableName() + " where " + DrsResourceLogHelper.LOG_TYPE + " ='" + logType + "'" + " AND " + DrsResourceLogHelper.APPLICATION_NAME + " = \'" + applicationId + "\'";
    }

    @Override
    public String getRequestMemorySql(TableSchema schema, String applicationId) {
// select * from DRS_PHASE where LOG_TYPE = 'MEMORY_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select " + DrsPhaseLogHelper.RUN_TIME + " from " + schema.getTableName() + " where " + DrsPhaseLogHelper.LOG_TYPE + " = \'" + MonitorConstant.MEMORY_REQUEST + "\' AND APPLICATION_NAME = \'" + applicationId + "\'";
    }

    @Override
    public String getRequestVcoresSql(TableSchema schema, String applicationId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select " + DrsPhaseLogHelper.RUN_TIME + " from " + schema.getTableName() + " where " + DrsPhaseLogHelper.LOG_TYPE + " = \'" + MonitorConstant.CORES_REQUEST + "\' AND APPLICATION_NAME = \'" + applicationId + "\'";
    }

    @Override
    public String getAvgCpuUsageSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select AVG(" + DrsResourceLogHelper.CPU_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getAvgJvmMemSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select AVG(" + DrsResourceLogHelper.JVM_MEM_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getAvgProcMemSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select AVG(" + DrsResourceLogHelper.PROC_MEM_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getMaxCpuUsageSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select MAX(" + DrsResourceLogHelper.CPU_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getMaxJvmMemSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select MAX(" + DrsResourceLogHelper.JVM_MEM_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getMaxProcMemSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select MAX(" + DrsResourceLogHelper.PROC_MEM_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getMaxRMemSql(TableSchema schema, String containerId) {
//     select * from DRS_PHASE where LOG_TYPE = 'CORES_REQUEST' AND APPLICATION_NAME = 'application_1487537926652_0027';
      return "select MAX(" + DrsResourceLogHelper.PROC_MEM_USAGE + " - " + DrsResourceLogHelper.JVM_MEM_USAGE + ") from " + schema.getTableName() + " where  CONTAINER_ID = \'" + containerId + "\'";
    }

    @Override
    public String getAvgPhaseRuntimeSql(TableSchema schema, String applicationId, String logType) {
      return "select AVG(" + DrsPhaseLogHelper.RUN_TIME + ") from " + schema.getTableName() + " where " + DrsResourceLogHelper.APPLICATION_NAME + " = \'" + applicationId + "\' AND " + DrsPhaseLogHelper.LOG_TYPE + " = \'" + logType + "\'";
    }

    @Override
    public String getAvgPhaseMemoryUsageSql(TableSchema schema, String applicationId, String logType) {
      return "select AVG(" + DrsResourceLogHelper.JVM_MEM_USAGE + ") from " + schema.getTableName() + " where " + DrsResourceLogHelper.APPLICATION_NAME + " = \'" + applicationId + "\' AND " + DrsResourceLogHelper.LOG_TYPE + " = \'" + logType + "\'";

    }

    @Override
    public String getAvgPhaseCpuRateSql(TableSchema schema, String applicationId, String logType) {
      return "select AVG(" + DrsResourceLogHelper.CPU_USAGE + ") from " + schema.getTableName() + " where " + DrsResourceLogHelper.APPLICATION_NAME + " = \'" + applicationId + "\' AND " + DrsResourceLogHelper.LOG_TYPE + " = \'" + logType + "\'";

    }

    @Override
    public String getApplicationResourceInfosSql(TableSchema schema, String applicationId) {
      String cpuUsage = DrsResourceLogHelper.CPU_USAGE;
      String procMemUsage = DrsResourceLogHelper.PROC_MEM_USAGE;
      String jvmMemUsage = DrsResourceLogHelper.JVM_MEM_USAGE;

      return "select AVG(" + cpuUsage + ") as AVG_CPU_USAGE,"
              + "MAX(" + cpuUsage + ") as MAX_CPU_USAGE,"
              + "AVG(" + procMemUsage + ") as AVG_PROC_MEM,"
              + "MAX(" + procMemUsage + ") as MAX_PROC_MEM,"
              + "AVG(" + jvmMemUsage + ") as AVG_JVM_MEM,"
              + "MAX(" + jvmMemUsage + ") as MAX_JVM_MEM,"
              + "AVG(" + procMemUsage + "-" + jvmMemUsage + ") as AVG_R_MEM,"
              + "MAX(" + procMemUsage + "-" + jvmMemUsage + ") as MAX_R_MEM"
              + " from " + schema.getTableName() + " where " + DrsResourceLogHelper.APPLICATION_NAME + " = '" + applicationId + "'"
              + " AND " + DrsResourceLogHelper.CONTAINER_ID + " not like \'%000001\'";
    }

  }
}
