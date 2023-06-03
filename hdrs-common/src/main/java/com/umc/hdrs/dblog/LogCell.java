package com.umc.hdrs.dblog;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class LogCell {

  private final Map<String, SqlCell> sqlCells = new HashMap<>();

  public static enum SqlType {
    BIGINT,
    INTEGER,
    STRING,
    TEXT,
    DATETIME;
  };

  public static SqlCell<Long> valueOfLong(String colName, Long value) {
    return new SqlCell<>(colName, value, SqlType.BIGINT);
  }

  public static SqlCell<Long> valueOfTime(String colName, Long value) {
    return new SqlCell<>(colName, value, SqlType.DATETIME);
  }

  public static SqlCell<Integer> valueOfInteger(String colName, Integer value) {
    return new SqlCell<>(colName, value, SqlType.INTEGER);
  }

  public static SqlCell<String> valueOfString(String colName, String value) {
    return new SqlCell<>(colName, value, SqlType.STRING);
  }

  public static SqlCell<String> valueOfText(String colName, String value) {
    return new SqlCell<>(colName, value, SqlType.TEXT);
  }

  public final static class SqlCell<T> {
    private final String colName;
    private final T value;
    private final SqlType sqlType;

    private SqlCell(String colName, T value, SqlType sqlType) {
      this.colName = colName;
      this.value = value;
      this.sqlType = sqlType;
    }

    public String getColName() {
      return colName;
    }

    public T getCellVal() {
      return value;
    }

    public SqlType getType() {
      return sqlType;
    }
  }

  public void addSQLCell(SqlCell s) {
    sqlCells.putIfAbsent(s.getColName(), s);
  }

  public Optional<SqlCell> getLog(String colName) {
    return Optional.ofNullable(sqlCells.get(colName));
  }

  @VisibleForTesting
  Map<String, SqlCell> getLog() {
    return sqlCells;
  }
}
