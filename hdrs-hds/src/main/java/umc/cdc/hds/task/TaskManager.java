/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
 */
package umc.cdc.hds.task;

import com.umc.hdrs.dblog.DbLogger;
import com.umc.hdrs.dblog.EmptyDbLogger;
import com.umc.hdrs.dblog.Logger;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.lib.HdsMetrics;
import org.apache.hadoop.metrics2.lib.HdsMetricsSource;
import umc.cdc.hds.core.HBaseConnection;
import static umc.cdc.hds.core.HDS.getDataStorage;
import static umc.cdc.hds.core.HDS.getReadLock;
import static umc.cdc.hds.core.HDS.getWriteLock;
import umc.cdc.hds.core.HDSConstants;
import static umc.cdc.hds.core.HDSConstants.URL_ENABLE_WILDCARD;
import umc.cdc.hds.core.HDSUtil;
import umc.cdc.hds.core.LockableInput;
import umc.cdc.hds.core.LockableOutput;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.core.SingletonConnectionFactory;
import umc.cdc.hds.datastorage.DataStorage;
import umc.cdc.hds.datastorage.DataStorageInput;
import umc.cdc.hds.datastorage.DataStorageOutput;
import static umc.cdc.hds.datastorage.HdsDataStorage.getComparable;
import umc.cdc.hds.datastorage.status.ErrorInfo;
import umc.cdc.hds.dblog.HdsLog;
import umc.cdc.hds.dblog.HdsLogHelper;
import umc.cdc.hds.httpserver.Exchange;
import umc.cdc.hds.httpserver.Result;
import umc.cdc.hds.task.TaskInfoBuilder.TaskInfo;
import umc.cdc.hds.lock.Lock;
import umc.cdc.hds.metric.HDSMetricSystem;
import umc.cdc.hds.metric.MetricCounter;
import umc.cdc.hds.metric.MetricSystem;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.tools.StopWatch;
import umc.cdc.hds.tools.ThreadPoolManager;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriRequestExchange;
import umc.udp.core.framework.AtomicCloseable;
import umc.udp.core.framework.ShareableObject;

/**
 *
 * @author brandboat
 */
public final class TaskManager {

  private static final ConcurrentSkipListMap<String, Task> TASKLIST = new ConcurrentSkipListMap<>();
  private static final int INDEX = 1000;
  private static final Random RANDOM = new Random(INDEX);
  private static final CountDownLatch WAITLATCH = new CountDownLatch(1);
  private static final AtomicBoolean ISINITIALIZED = new AtomicBoolean();
  private static final Log LOG = LogFactory.getLog(TaskManager.class);
  private static final AtomicInteger TASKCOUNTER = new AtomicInteger(0);
  private static TaskRecorder TASKRECORDER;
  private static Configuration CONF = new Configuration();
  private static ShareableObject<MetricSystem> metricSystem;
  private static ShareableObject<HdsMetrics> hdsMetrics;
  private static final String SPLIT = "%";
  private static final long REVERSE = "Sweatshop".hashCode();
  private static int TASK_MAX;
  private static Logger dbLogger;

  private TaskManager() {
  }

  public static void initialize(final Configuration config, ShareableObject<HdsMetrics> metrics)
      throws Exception {
    if (ISINITIALIZED.compareAndSet(false, true)) {
      try {
        CONF = config;
        TASK_MAX = config.getInt(
            HDSConstants.HTTP_SERVER_LONG_TERM_TASK_NUM,
            HDSConstants.DEFAULT_HTTP_SERVER_LONG_TERM_TASK_NUM);
        metricSystem = HDSMetricSystem.getInstance(CONF);
        hdsMetrics = metrics;
        dbLogger = config.getBoolean(
            HDSConstants.LOGGER_ENABLE, HDSConstants.DEFAULT_LOGGER_ENABLE)
                ? HdsLogHelper.getDbLogger(HdsLogHelper.getInfo(), new DbLogger.DbLogConfig(config))
                : EmptyDbLogger.getDbLogger();
      } finally {
        WAITLATCH.countDown();
      }
    } else {
      WAITLATCH.await();
    }
  }

  public static int getTaskNum() {
    return TASKCOUNTER.get();
  }

  public static enum TaskStateEnum {

    PENDING,
    TRIGGERED,
    RUNNING,
    SUCCEED,
    FAILED;

    public static Optional<TaskStateEnum> find(String state) {
      if (state == null) {
        return Optional.empty();
      }
      for (TaskStateEnum p : TaskStateEnum.values()) {
        if (p.name().equalsIgnoreCase(state)) {
          return Optional.of(p);
        }
      }
      return Optional.empty();
    }
  }

  private static synchronized void restrictTaskNum() throws IOException {
    try {
      if (TASKCOUNTER.incrementAndGet() > TASK_MAX) {
        throw new IOException("Task is full now.");
      }
    } catch (IOException ex) {
      TASKCOUNTER.decrementAndGet();
      throw ex;
    }
  }

  public static Task createTask(
      UriRequestExchange reqFrom,
      UriRequestExchange reqTo,
      Optional<String> redirectFrom,
      boolean isAsync,
      HdsLog hdsLog) throws IOException {
    restrictTaskNum();
    Task task = new Task(
        createTaskId(),
        reqFrom,
        reqTo,
        redirectFrom,
        isAsync,
        hdsLog);
    TaskManager.putTask(task);
    return task;
  }

  public static TaskInfo abortTask(String id) throws Exception {
    Task t = TASKLIST.get(id);
    if (t == null) {
      throw new Exception("Task: " + id + " not found.");
    } else {
      t.abort();
      TaskInfo ti = t.getTaskInfo();
      return ti;
    }
  }

  private static void putTask(Task task) {
    TASKLIST.put(task.getId(), task);
  }

  private static void removeTask(Task task) {
    TASKLIST.remove(task.getId());
  }

  public static CloseableIterator<TaskInfo> getTaskInfo(Query query) throws IOException {
    TaskQuery tq = new TaskQuery(query.getQueries());
    TaskManager.initializeTaskRecorder();
    return TASKRECORDER.list(tq);
  }

  private static TaskInfo getTaskInfo(String id) {
    return TASKLIST.get(id).getTaskInfo();
  }

  private static String getHostName() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      hostName = "localhost";
    }
    return hostName;
  }

  private static byte[] createRowKey(String id) {
    final String[] idarr = id.split("\\" + SPLIT);
    final long reverseTime = REVERSE - Long.parseLong(idarr[0]);
    final String hostName = idarr[1];
    final int randomNum = Integer.parseInt(idarr[2]);
    final byte[] reverse = Bytes.toBytes(reverseTime);
    final byte[] host = Bytes.toBytes(hostName);
    final byte[] random = Bytes.toBytes(randomNum);
    final byte[] idByte = new byte[reverse.length + host.length + random.length];
    System.arraycopy(reverse, 0, idByte, 0, reverse.length);
    System.arraycopy(host, 0, idByte, reverse.length, host.length);
    System.arraycopy(random, 0, idByte, host.length, random.length);
    return idByte;
  }

  private static String createTaskId() {
    StringBuilder str = new StringBuilder();
    str.append(System.currentTimeMillis())
        .append(SPLIT)
        .append(getHostName())
        .append(SPLIT)
        .append(RANDOM.nextInt(INDEX));
    return str.toString();
  }

  public static interface State {

    public void schedule();

    public void execute() throws Exception;

    public void retry();

    public Result lastStep();

    public void cancel();

    TaskStateEnum getStateEnum();
  }

  public static class AbstractState implements State {

    protected Task task;
    protected TaskStateEnum stateEnum;

    public AbstractState(Task task, TaskStateEnum stateEnum) {
      this.task = task;
      this.stateEnum = stateEnum;
    }

    @Override
    public void schedule() {
      LOG.info("Task[" + task.getId() + "] can't schedule from " + stateEnum);
    }

    @Override
    public void execute() throws Exception {
      LOG.info("Task[" + task.getId() + "] can't execute from " + stateEnum);
    }

    @Override
    public void retry() {
      LOG.info("Task[" + task.getId() + "] can't retry from " + stateEnum);
    }

    @Override
    public Result lastStep() {
      return new Result(Status.OK, task.getTaskInfo(), HDSConstants.TASK);
    }

    @Override
    public void cancel() {
      LOG.info("Task[" + task.getId() + "] can't cancel from " + stateEnum);
    }

    @Override
    public TaskStateEnum getStateEnum() {
      return stateEnum;
    }

  }

  /**
   * Create Task Recorder when the first access api was triggered.
   *
   * @throws RuntimeException
   */
  public static void initializeTaskRecorder() throws RuntimeException {
    try {
      if (TASKRECORDER == null) {
        TASKRECORDER = TaskRecorder.getInstance();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static class InitState extends AbstractState {

    private final HdsLog hdsLog;

    public InitState(Task task, HdsLog hdsLog) {
      super(task, TaskStateEnum.PENDING);
      this.hdsLog = hdsLog;
      initializeTaskRecorder();
    }

    @Override
    public void schedule() {
      task.switchState(new TriggeredState(task, hdsLog));
    }

  }

  public static class TriggeredState extends AbstractState {

    private HdsLog hdsLog;

    public TriggeredState(TaskManager.Task task, HdsLog hdsLog) {
      super(task, TaskManager.TaskStateEnum.TRIGGERED);
      this.hdsLog = hdsLog;
    }

    @Override
    public void execute() {
      if (task.isAsync()) {
        ThreadPoolManager.execute(() -> {
          innerExecute();
          try {
            hdsLog.waitForResponseDone();
          } catch (InterruptedException ex) {
            LOG.error("HdsLog async task interrupted.");
          }
          dbLogger.addLog(HdsLogHelper.toLogCell(hdsLog));
        });
      } else {
        innerExecute();
      }
    }

    private void innerExecute() {
      try {
        task.action();
        task.switchState(
            new FinalState(task,
                TaskManager.TaskStateEnum.SUCCEED));
      } catch (Exception ex) {
        task.switchState(
            new FinalState(task,
                TaskManager.TaskStateEnum.FAILED, ex));
      }
    }
  }

  public static class FinalState extends AbstractState {

    private final Exception ex;

    public FinalState(Task task, TaskStateEnum state) {
      super(task, state);
      this.ex = null;
      task.setEndTime(System.currentTimeMillis());
      TASKCOUNTER.decrementAndGet();
    }

    public FinalState(Task task, TaskStateEnum state, Exception ex) {
      super(task, state);
      this.ex = ex;
      task.setEndTime(System.currentTimeMillis());
      TASKCOUNTER.decrementAndGet();
    }

    @Override
    public Result lastStep() {
      Result result;
      switch (stateEnum) {
        case FAILED:
          result = new Result(
              Status.INTERNAL_SERVER_ERROR,
              new ErrorInfo(ex), HDSConstants.ERROR_RESPONSE);
          break;
        case SUCCEED:
          result = new Result(
              Status.OK, task.getTaskInfo(), HDSConstants.TASK);
          break;
        default:
          result = new Result(
              Status.OK, task.getTaskInfo(), HDSConstants.TASK);
          break;
      }
      return result;
    }
  }

  private static class MetricsDataStorageInput extends DataStorageInput {

    private final DataStorageInput dsInput;
    private final MetricCounter counter;
    private final UriRequestExchange request;

    MetricsDataStorageInput(DataStorageInput dsInput, UriRequestExchange request)
        throws IOException {
      this.dsInput = dsInput;
      this.request = request;
      this.counter = dsInput.getSize() == 0
          ? metricSystem.get().registerUnExpectableCounter(HDSConstants.LOADINGCATEGORY.READ)
          : metricSystem.get().registeExpectableCounter(
              HDSConstants.LOADINGCATEGORY.READ, dsInput.getSize());
    }

    @Override
    public void close() throws IOException {
      try {
        dsInput.close();
      } finally {
        counter.close();
      }
    }

    @Override
    public int read() throws IOException {
      int result = dsInput.read();
      if (result != -1) {
        counter.addBytes(1L);
      }
      return result;
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      int readByte = dsInput.read(b, offset, length);
      if (readByte > 0) {
        counter.addBytes(readByte);
      }
      return readByte;
    }

    @Override
    public void recover() {
      dsInput.recover();
    }

    @Override
    public long getSize() {
      return dsInput.getSize();
    }

    @Override
    public String getName() {
      return dsInput.getName();
    }

    @Override
    public String getUri() {
      return dsInput.getUri();
    }
  }

  private static class MetricsDataStorageOutput extends DataStorageOutput {

    private final DataStorageOutput dsOutput;
    private final MetricCounter counter;
    private final UriRequestExchange request;

    MetricsDataStorageOutput(DataStorageOutput dsOutput, UriRequestExchange request) throws IOException {
      this.dsOutput = dsOutput;
      this.request = request;
      counter = metricSystem.get().registerUnExpectableCounter(HDSConstants.LOADINGCATEGORY.WRITE);
    }

    @Override
    public void close() throws IOException {
      try {
        dsOutput.close();
      } finally {
        counter.close();
      }
    }

    @Override
    public void recover() throws IOException {
      dsOutput.recover();
    }

    @Override
    public void write(int b) throws IOException {
      dsOutput.write(b);
      counter.addBytes(1l);
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      dsOutput.write(b, offset, length);
      counter.addBytes(length);
    }

    @Override
    public long getSize() {
      return dsOutput.getSize();
    }

    @Override
    public String getName() {
      return dsOutput.getName();
    }

    @Override
    public String getUri() {
      return dsOutput.getUri();
    }

    @Override
    public void commit() throws IOException {
      dsOutput.commit();
    }

  }

  /**
   * WARN: TaskRecorder is used to update hbase table. 1. TASK Table 2. TASKLOG
   * Table It should not be created just for once. Also you hava to wait until
   * hbase regionserver are already up then new it.
   */
  private static final class TaskRecorder extends AtomicCloseable {

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static TaskRecorder RECORDER;
    private final HBaseConnection conn;
    private final int ttl = (int) TimeUnit.MILLISECONDS.toSeconds(
        CONF.getLong(
            HDSConstants.TASK_HEARTBEAT,
            HDSConstants.DEFAULT_TASK_HEARTBEAT));
    private final int updatePeriod = ttl;

    private TaskRecorder() throws IOException {
      conn = SingletonConnectionFactory.createConnection(CONF);
      createTaskTable();
      createLogTable();
      setThreads();
    }

    public static TaskRecorder getInstance() throws IOException {
      synchronized (INITIALIZED) {
        if (INITIALIZED.compareAndSet(false, true)) {
          RECORDER = new TaskRecorder();
        }
        return RECORDER;
      }
    }

    private CloseableIterator<TaskInfo> list(TaskQuery tq) throws IOException {
      final Scan scan = createScan(tq);
      final Table table = tq.getEnableHistory() ? getLogTable() : getTaskTable();
      return new CloseableIterator<TaskInfo>() {
        private final ResultScanner rs = table.getScanner(scan);
        private int position = 0;
        private org.apache.hadoop.hbase.client.Result next = rs.next();
        private final int offset = tq.getOffset();
        private final int limit = tq.getLimit();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public TaskInfo next() {
          if (next == null) {
            return null;
          }

          TaskInfo ti;
          try {
            position++;
            while (position < offset + 1) {
              position++;
              next = rs.next();
            }
            if (position - offset > limit) {
              next = null;
              return null;
            }
            ti = new TaskInfoBuilder().build(next);
            next = rs.next();
          } catch (IOException | RuntimeException e) {
            throw new RuntimeException(e);
          }
          return ti;
        }

        @Override
        public void close() {
          rs.close();
          HDSUtil.closeWithLog(table, LOG);
        }
      };
    }

    private Scan createScan(final TaskQuery tq) {
      boolean enableWildcard
          = tq.getQueryValueAsBoolean(URL_ENABLE_WILDCARD, false);
      FilterList filters = new FilterList();
      tq.getId().ifPresent(v
          -> filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_ID_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(v, enableWildcard))));
      tq.getServerName().ifPresent(v
          -> filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_SERVERNAME_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(v, enableWildcard))));
      tq.getClientName().ifPresent(v
          -> filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_CLIENTNAME_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(v, enableWildcard))));
      tq.getFrom().ifPresent(v
          -> filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_FROM_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(v, enableWildcard))));
      tq.getTo().ifPresent(v
          -> filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_TO_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(v, enableWildcard))));
      tq.getState().ifPresent(v
          -> filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_STATE_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(v.name(), enableWildcard))));
      if (tq.getProgress().isPresent()) {
        tq.getProgress().ifPresent(v
            -> filters.addFilter(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                TaskConstants.TASK_PROGRESS_QUALIFIER,
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(v))));
      } else {
        tq.getProgressRange().ifPresent(v -> {
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_PROGRESS_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_PROGRESS_QUALIFIER,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              Bytes.toBytes(v.getMaxValue())));
        });
      }
      if (tq.getStartTime().isPresent()) {
        tq.getStartTime().ifPresent(v
            -> filters.addFilter(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                TaskConstants.TASK_STARTTIME_QUALIFIER,
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(v))));
      } else {
        tq.getStartTimeRange().ifPresent(v -> {
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_STARTTIME_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_STARTTIME_QUALIFIER,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              Bytes.toBytes(v.getMaxValue())));
        });
      }
      if (tq.getElapsed().isPresent()) {
        tq.getElapsed().ifPresent(v
            -> filters.addFilter(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                TaskConstants.TASK_ELAPSED_QUALIFIER,
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(v))));
      } else {
        tq.getElapsedRange().ifPresent(v -> {
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_ELAPSED_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_ELAPSED_QUALIFIER,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              Bytes.toBytes(v.getMaxValue())));
        });
      }
      if (tq.getExpectedSize().isPresent()) {
        tq.getExpectedSize().ifPresent(v
            -> filters.addFilter(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                TaskConstants.TASK_ELAPSED_QUALIFIER,
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(v))));
      } else {
        tq.getExpectedSizeRange().ifPresent(v -> {
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_EXPECTEDSIZE_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
          filters.addFilter(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              TaskConstants.TASK_EXPECTEDSIZE_QUALIFIER,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              Bytes.toBytes(v.getMaxValue())));
        });
      }
      Scan scan = new Scan();
      if (!filters.getFilters().isEmpty()) {
        scan.setFilter(filters);
      }
      return scan;
    }

    private void createTaskTable() throws IOException {
      try (Admin admin = conn.getAdmin()) {
        if (!admin.tableExists(TaskConstants.TASK_TABLE)) {
          HDSUtil.createNamespaceIfNotExisted(conn,
              TaskConstants.TASK_TABLE.getNamespaceAsString());
          HTableDescriptor watchTableMeta
              = new HTableDescriptor(TaskConstants.TASK_TABLE);
          watchTableMeta.setDurability(Durability.SKIP_WAL);
          HColumnDescriptor cf
              = new HColumnDescriptor(HDSConstants.DEFAULT_FAMILY);
          cf.setMaxVersions(1);
          cf.setTimeToLive(ttl);
          cf.setCacheBloomsOnWrite(true);
          cf.setCacheIndexesOnWrite(true);
          watchTableMeta.addFamily(cf);
          admin.createTable(watchTableMeta);
        }
      } catch (TableExistsException ex) {
        // cluster mode may create duplicate table. Ex.
        // multi task in same time.
      }
    }

    private Table getTaskTable() throws IOException {
      return conn.getTable(TaskConstants.TASK_TABLE);
    }

    private void createLogTable() throws IOException {
      try (Admin admin = conn.getAdmin()) {
        if (!admin.tableExists(TaskConstants.TASK_LOG_TABLE)) {
          HDSUtil.createNamespaceIfNotExisted(conn, TaskConstants.TASK_LOG_TABLE.getNamespaceAsString());
          HTableDescriptor meta
              = new HTableDescriptor(TaskConstants.TASK_LOG_TABLE);
          meta.setCompactionEnabled(true);
          HColumnDescriptor cf
              = new HColumnDescriptor(HDSConstants.DEFAULT_FAMILY);
          cf.setCompactionCompressionType(Compression.Algorithm.GZ);
          cf.setCompressionType(Compression.Algorithm.GZ);
          meta.addFamily(cf);
          admin.createTable(meta);
        }
      } catch (TableExistsException ex) {
      }
    }

    private Table getLogTable() throws IOException {
      return conn.getTable(TaskConstants.TASK_LOG_TABLE);
    }

    private void setThreads() {
      ThreadPoolManager.scheduleWithFixedDelay(() -> {
        try {
          updateTaskAndLogTable();
        } catch (Exception ex) {
          LOG.error("Loading update thread was interrupted.", ex);
        }
      }, updatePeriod, TimeUnit.SECONDS);
    }

    private void updateTaskAndLogTable() throws IOException {
      List<Put> taskPuts = new ArrayList<>();
      List<Put> logPuts = new ArrayList<>();
      List<Delete> deletes = new ArrayList<>();
      List<String> removeTasks = new ArrayList<>();
      try (Table table = getTaskTable()) {
        for (String id : TASKLIST.keySet()) {
          Task t = TASKLIST.get(id);
          TaskInfo ti = t.getTaskInfo();
          // Since the expectedsize may not be initialized yet, we have to
          // update the taskinfo after all taskinfo needed info is ready
          // there is a stateenum called "RUNNING" which means the data is
          // already set.
          if (ti.getStateEnum().equals(TaskStateEnum.PENDING)
              || ti.getStateEnum().equals(TaskStateEnum.TRIGGERED)) {
            continue;
          }
          byte[] rowKey = createRowKey(id);

          Put put = new Put(rowKey);
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_ID_QUALIFIER,
              Bytes.toBytes(id));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_REDIRECTFROM_QUALIFIER,
              Bytes.toBytes(ti.getRedirectFrom()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_SERVERNAME_QUALIFIER,
              Bytes.toBytes(ti.getServerName()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_CLIENTNAME_QUALIFIER,
              Bytes.toBytes(ti.getClientName()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_FROM_QUALIFIER,
              Bytes.toBytes(ti.getFrom()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_TO_QUALIFIER,
              Bytes.toBytes(ti.getTo()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_STATE_QUALIFIER,
              Bytes.toBytes(ti.getStateEnum().name()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_PROGRESS_QUALIFIER,
              Bytes.toBytes(ti.getProgress()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_STARTTIME_QUALIFIER,
              Bytes.toBytes(ti.getStartTime()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_ELAPSED_QUALIFIER,
              Bytes.toBytes(ti.getElapsedTime()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_EXPECTEDSIZE_QUALIFIER,
              Bytes.toBytes(ti.getDataSize()));
          put.addColumn(HDSConstants.DEFAULT_FAMILY, TaskConstants.TASK_TRANSFERREDSIZE_QUALIFIER,
              Bytes.toBytes(ti.getDataTransferSize()));
          if (ti.getStateEnum().equals(TaskStateEnum.SUCCEED)
              || ti.getStateEnum().equals(TaskStateEnum.FAILED)) {
            removeTasks.add(id);
            Delete del = new Delete(rowKey);
            deletes.add(del);
            logPuts.add(put);
          } else {
            taskPuts.add(put);
          }
        }
        // remove success and fail task.
        removeTasks.forEach(id -> {
          TASKLIST.remove(id);
        });
        if (!taskPuts.isEmpty()) {
          table.put(taskPuts);
        }
        if (!deletes.isEmpty()) {
          table.delete(deletes);
        }
        updateTaskLogTable(logPuts);
      }
    }

    private void updateTaskLogTable(List<Put> puts) throws IOException {
      try (Table table = getLogTable()) {
        table.put(puts);
      }
    }

    @Override
    protected void internalClose() {
      HDSUtil.closeWithLog(conn, LOG);
    }
  }

  public static class Task implements State {

    private final UriRequestExchange reqFrom;
    private final UriRequestExchange reqTo;
    private Optional<DataStorageInput> idsInput = Optional.empty();
    private Optional<DataStorageOutput> idsOutput = Optional.empty();
    private final String id;
    private final Exchange exchange;
    private final Optional<String> redirectFrom;
    private TaskStateEnum stateEnum = TaskStateEnum.PENDING;
    private State state;
    private final AtomicBoolean isAbort = new AtomicBoolean(false);
    private final boolean isAsync;
    private final long startTime;
    private long endTime;
    private final HdsLog hdsLog;

    public Task(String id, UriRequestExchange reqFrom,
        UriRequestExchange reqTo, Optional<String> redirectFrom,
        boolean isAsync, HdsLog hdsLog) {
      this.id = id;
      this.reqFrom = reqFrom;
      this.reqTo = reqTo;
      this.exchange = new Exchange(reqFrom.getHttpExchange());
      this.redirectFrom = redirectFrom;
      this.isAsync = isAsync;
      this.hdsLog = hdsLog;
      this.startTime = hdsLog.getRequestStartTime();
      initState();
    }

    public void action() throws IOException {
      try {
        initialize();
        transfer();
      } catch (IOException ex) {
        recover();
        throw ex;
      }
    }

    public void switchState(State newState) {
      this.state = newState;
      this.stateEnum = newState.getStateEnum();
    }

    @Override
    public void schedule() {
      this.state.schedule();
    }

    @Override
    public void execute() throws Exception {
      this.state.execute();
    }

    @Override
    public void retry() {
      this.state.retry();
    }

    @Override
    public void cancel() {
      this.state.cancel();
    }

    public void abort() {
      isAbort.compareAndSet(false, true);
    }

    @Override
    public Result lastStep() {
      return this.state.lastStep();
    }

    public void initState() {
      switch (stateEnum) {
        case PENDING:
          this.state = new InitState(this, hdsLog);
          break;
        case RUNNING:
          this.state = new TriggeredState(this, hdsLog);
          break;
        case SUCCEED:
          this.state = new FinalState(this, TaskStateEnum.SUCCEED);
          break;
        case FAILED:
          this.state = new FinalState(this, TaskStateEnum.FAILED);
          break;
        default:
          this.state = new InitState(this, hdsLog);
      }
    }

    private void transfer() throws IOException {
      try {
        this.stateEnum = TaskStateEnum.RUNNING;
        int c;
        long readSize = 0;
        byte[] b = new byte[HDSConstants.DEFAULT_BUFFER_SIZE];
        StopWatch transferTime = new StopWatch();
        while ((c = idsInput.get().read(b)) >= 0) {
          if (isAbort.get()) {
            throw new IOException("Task is canceled.");
          }
          readSize += c;
          idsOutput.get().write(b, 0, c);
        }
        long elapsed = transferTime.getElapsed();
        hdsLog.setTransferDataElapsedTime(elapsed);

        elapsed = elapsed == 0 ? 1 : elapsed;
        // Rate : KB/s
        long speed = (readSize / elapsed) * 1000 / 1024;
        try {
          if (hdsMetrics != null) {
            hdsMetrics.get().updateReadWriteRate(
                Protocol.valueOf(reqFrom.getProtocol().name()), HdsMetricsSource.Rate.READ, speed);
            hdsMetrics.get().updateReadWriteRate(
                Protocol.valueOf(reqTo.getProtocol().name()), HdsMetricsSource.Rate.WRITE, speed);
          }
        } catch (Exception ex) {
          LOG.error(ex);
        }
        if (idsInput.get().getSize() != 0
            && idsInput.get().getSize() != readSize) {
          throw new IOException("Size not matched.");
        }

        StopWatch commitTime = new StopWatch();
        idsOutput.get().commit();
        hdsLog.setCommitElapsedTime(commitTime.getElapsed());

        hdsLog.setDataSize(readSize);
      } finally {
        closeResource();
      }
    }

    private void recover() {
      if (idsOutput.isPresent()) {
        try {
          idsOutput.get().recover();
        } catch (IOException ioex) {
          LOG.error(ioex);
        }
      }
    }

    private void closeResource() throws IOException {
      try {
        if (idsOutput.isPresent()) {
          idsOutput.get().close();
        }
      } finally {
        if (idsInput.isPresent()) {
          idsInput.get().close();
        }
      }
    }

    public void initialize()
        throws IOException {
      long getlockElapsed = 0;
      long getSourceElapsed = 0;

      try {
        Lock lockInput = null;
        DataStorageInput dsInput = null;
        try {
          StopWatch getLockTime = new StopWatch();
          lockInput = getReadLock(reqFrom);
          getlockElapsed += getLockTime.getElapsed();

          StopWatch getSourceTime = new StopWatch();

          dsInput = getDataStorage(reqFrom, CONF).open(reqFrom); //go to XXXDataStorage.java
          getSourceElapsed += getSourceTime.getElapsed();

          dsInput = new LockableInput(lockInput, dsInput, hdsLog);
          dsInput = new MetricsDataStorageInput(dsInput, reqFrom);
          idsInput = Optional.of(dsInput);
        } catch (IOException ex) {
          if (lockInput != null && !idsInput.isPresent()) {
            lockInput.close();
          }
          throw ex;
        }
        // if name is not specified in req to uri, we have to give it
        // the default name. Naming Rules are as follows:
        // 1. to_uri has name, no need to add.
        // 2. to_uri has no name, from_uri has name, give to_uri name.
        // if to_uri get data from hds, we have to get the name from hds
        // datastorage, other datastorage can get the name from uri.
        if (reqFrom.getProtocol().equals(Protocol.hds)) {
          if (reqTo.getQuery().getQueryValue(HDSConstants.HDS_URI_ARG_VERSION, 
                  HDSConstants.HDS_VERSION).equals(HDSConstants.HDS_VERSION_V2) 
                  && !reqTo.getPath().endsWith("/")) {
            // no_op
          } else if (!reqTo.getName().isPresent()) {
            reqTo.setName(idsInput.get().getName());
          }
        } else {
          reqFrom.getName().ifPresent(v -> {
            if (reqTo.getName().isPresent()) {
              return;
            }
            /*if (reqTo.getQuery().getQueryValue(HDSConstants.HDS_URI_ARG_VERSION,
                  HDSConstants.HDS_VERSION).equals(HDSConstants.HDS_VERSION_V2) 
                  && !reqTo.getPath().endsWith("/")) {
              return;
            }*/
            reqTo.setName(v);
          });
        }
        Lock lockOutput = null;
        DataStorageOutput dsOutput = null;
        try {
          StopWatch getLockTime = new StopWatch();
          lockOutput = getWriteLock(reqTo);
          getlockElapsed += getLockTime.getElapsed();

          StopWatch getSourceTime = new StopWatch();
          dsOutput = getDataStorage(reqTo, CONF).create(reqTo);
          getSourceElapsed += getSourceTime.getElapsed();

          hdsLog.setGetLockElapsedTime(getlockElapsed);
          hdsLog.setGetSourceElapsedTime(getSourceElapsed);

          dsOutput = new LockableOutput(lockOutput, dsOutput, hdsLog);
          dsOutput = new MetricsDataStorageOutput(dsOutput, reqTo);
          idsOutput = Optional.of(dsOutput);
        } catch (IOException ex) {
          if (lockOutput != null && !idsOutput.isPresent()) {
            lockOutput.close();
          }
          throw ex;
        }
      } catch (Exception ex) {
        closeResource();
        throw new IOException(ex);
      }
    }

    public void setEndTime(long endTime) {
      this.endTime = endTime;
    }

    public TaskInfo getTaskInfo() {
      return new TaskInfoBuilder()
          .setId(getId())
          .setStateEnum(getStateEnum())
          .setStartTime(getStartTime())
          .setElapsedTime(getElapsedTime())
          .setRedirectFrom(getRedirectFrom())
          .setClientName(getClientName())
          .setServerName(getServerName())
          .setDataSize(getDataSize())
          .setDataTransferSize(getDataTransferSize())
          .setProgress(getProgress())
          .setFrom(getFrom())
          .setTo(getTo())
          .build();
    }

    public Exchange getExchange() {
      return exchange;
    }

    public boolean isAsync() {
      return isAsync;
    }

    public String getId() {
      return id;
    }

    @Override
    public TaskStateEnum getStateEnum() {
      return stateEnum;
    }

    public String getFrom() {
      return reqFrom.toString();
    }

    public String getTo() {
      return reqTo.toString();
    }

    public long getDataSize() {
      if (idsInput.isPresent()) {
        return idsInput.get().getSize();
      } else {
        return 0;
      }
    }

    public long getDataTransferSize() {
      if (idsOutput.isPresent()) {
        return idsOutput.get().getSize();
      } else {
        return 0;
      }
    }

    // exchange.getLocalAddress.getHostName always get 0.0.0.0
    public String getServerName() throws RuntimeException {
      String host = "localhost";
      try {
        host = CONF.get(
            HDSConstants.HTTP_SERVER_ADDRESS,
            InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException ex) {
        throw new RuntimeException(ex);
      }
      return host;
    }

    public String getClientName() {
      return exchange.getRemoteAddress().getHostName();
    }

    public double getProgress() {
      long dataSize = getDataSize();
      if (dataSize <= 0) {
        return 0;
      }
      return (double) getDataTransferSize() / (double) dataSize;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getElapsedTime() {
      return System.currentTimeMillis() - startTime;
    }

    public String getRedirectFrom() {
      return redirectFrom.orElse("");
    }
  }

  public static void close() {
    HDSUtil.closeWithLog(metricSystem, LOG);
    HDSUtil.closeWithLog(TASKRECORDER, LOG);
  }
}
