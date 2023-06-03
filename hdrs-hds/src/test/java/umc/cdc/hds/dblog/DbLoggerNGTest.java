package umc.cdc.hds.dblog;

import com.umc.hdrs.dblog.DbLogger.SqlMSSqlServer;
import com.umc.hdrs.dblog.DbLogger.SqlMySql;
import com.umc.hdrs.dblog.DbLogger.SqlOracle;
import com.umc.hdrs.dblog.DbLogger.SqlPhoenix;
import com.umc.hdrs.dblog.DbLogger.SqlPostgres;
import com.umc.hdrs.dblog.DbLogger.TableSchema;
import com.umc.hdrs.dblog.DbLogger.TableSchemaBuilder;
import com.umc.hdrs.dblog.LogCell;
import com.umc.hdrs.dblog.LogCell.SqlType;
import java.io.IOException;
import org.testng.annotations.Test;

public class DbLoggerNGTest {

  private final SqlMySql sqlMySql = new SqlMySql();
  private final SqlOracle sqlOracle = new SqlOracle();
  private final SqlMSSqlServer sqlMSSql = new SqlMSSqlServer();
  private final SqlPostgres sqlPostgres = new SqlPostgres();
  private final SqlPhoenix sqlPhoenix = new SqlPhoenix();
  private final TableSchema info;
  private final LogCell log;

  public DbLoggerNGTest() throws IOException {
    TableSchemaBuilder builder = new TableSchemaBuilder();
    builder.setTableName("brandboat");
    builder.addColumn("col1", SqlType.BIGINT);
    builder.addColumn("col2", SqlType.DATETIME);
    builder.addColumn("col3", SqlType.INTEGER);
    builder.addColumn("col4", SqlType.STRING);
    builder.addColumn("col5", SqlType.TEXT);
    info = builder.build();
    log = new LogCell();
    log.addSQLCell(LogCell.valueOfLong("col1", System.currentTimeMillis()));
    log.addSQLCell(LogCell.valueOfTime("col2", null));
    log.addSQLCell(LogCell.valueOfInteger("col3", null));
    log.addSQLCell(LogCell.valueOfString("col4", null));
    log.addSQLCell(LogCell.valueOfText("col5", null));
  }

  @Test
  public void testCreateTableSQL() throws IOException {
    TableSchema ts = HdsLogHelper.getInfo();
    System.out.println("[MySQL]");
    System.out.println(sqlMySql.createTableSql(ts));
    System.out.println("[SQLServer]");
    System.out.println(sqlMSSql.createTableSql(ts));
    System.out.println("[Oracle]");
    System.out.println(sqlOracle.createTableSql(ts));
    System.out.println("[Phoenix]");
    System.out.println(sqlPhoenix.createTableSql(ts));
    System.out.println("[PostgreSQL]");
    System.out.println(sqlPostgres.createTableSql(ts));
  }

//    @Test
//    public void testBatchInsertMySql() throws IOException, ClassNotFoundException, SQLException {
//
//        DbAction action = new DbActionImpl(sqlMySql,
//                info,
//                "jdbc:mysql://140.116.245.138:3306/mysql?user=brandboat&password=brandboat",
//                "com.mysql.jdbc.Driver",
//                20);
//        action.dropTableIfExist();
//        action.createTableIfNotExist();
//
//        LogStore store = new DbLogger.LogStore(1000);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        action.batchInsert(store);
//        Assert.assertEquals(store.isEmpty(), true);
//    }
//    @Test
//    public void testBatchInsertOracleSql() throws IOException, ClassNotFoundException, SQLException {
//        info.setPeriod(0);
//        info.setJdbcUrl("jdbc:oracle:thin:c##brandboat/brandboat@//140.116.245.138:1521/orcl");
//        info.setJdbcDriver("oracle.jdbc.driver.OracleDriver");
//        DbAction action = new DbActionImpl(sqlOracle, info);
//        action.dropTableIfExist();
//        action.createTableIfNotExist();
//
//        LogStore store = new LogStore(1000);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        action.batchInsert(store);
//        Assert.assertEquals(store.isEmpty(), true);
//    }
//    @Test
//    public void testBatchInsertSqlServer() throws IOException, ClassNotFoundException, SQLException {
//        info.setPeriod(0);
//        info.setJdbcUrl("jdbc:sqlserver://140.116.245.141:1433;databaseName=test;user=sa;password=dslab");
//        info.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//        DbAction action = new DbActionImpl(sqlMSSql, info);
//        action.dropTableIfExist();
//        action.createTableIfNotExist();
//
//        LogStore store = new LogStore(1000);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        action.batchInsert(store);
//        Assert.assertEquals(store.isEmpty(), true);
//    }
//
//    @Test
//    public void testBatchInsertPostgres() throws IOException, ClassNotFoundException, SQLException {
//
//        DbAction action = new DbActionImpl(sqlPostgres,
//        info,
//        "jdbc:datadirect:postgresql://192.168.103.30:5432;User=dslab;Password=dslab65a03",
//        "com.ddtek.jdbc.postgresql.PostgreSQLDriver",
//        20);
//        action.dropTableIfExist();
//        action.createTableIfNotExist();
//
//        LogStore store = new DbLogger.LogStore(1000);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        action.batchInsert(store);
//        Assert.assertEquals(store.isEmpty(), true);
//
//        action.dropTableIfExist();
//        action.createTableIfNotExist();
//
//        action.batchInsert(store);
//        Assert.assertEquals(store.isEmpty(), true);
//    }
//    @Test
//    public void testBatchInsertPhoenix() throws IOException, ClassNotFoundException, SQLException {
//        DbAction action = new DbActionImpl(sqlPhoenix,
//                info,
//                "jdbc:phoenix:192.168.103.31:2181",
//                "org.apache.phoenix.jdbc.PhoenixDrive",
//                20);
//        action.dropTableIfExist();
//        action.createTableIfNotExist();
//
//        LogStore store = new DbLogger.LogStore(1000);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        store.addLog(log);
//        action.batchInsert(store);
//        Assert.assertEquals(store.isEmpty(), true);
//    }
}
