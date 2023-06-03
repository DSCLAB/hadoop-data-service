package umc.cdc.hds.httpserver;

import org.apache.http.client.utils.URIBuilder;
import umc.cdc.hds.datastorage.JdbcConnectionManager;
import umc.cdc.hds.datastorage.status.filter.DatabaseInfo;
import umc.cdc.hds.exceptions.JdbcDataStorageException;
import umc.cdc.hds.task.TaskManager;
import java.io.IOException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.metrics2.lib.HdsMetrics;
import com.dslab.drs.api.DRSClient;
import com.dslab.drs.api.KilldrsResponseMessage;
import com.dslab.drs.api.ServiceStatus;
import com.dslab.drs.exception.DrsClientException;
import com.dslab.drs.exception.ApplicationIdNotFoundException;
import com.umc.hdrs.dblog.DbLogger.DbLogConfig;
import com.umc.hdrs.dblog.EmptyDbLogger;
import com.umc.hdrs.dblog.Logger;
import java.nio.ByteBuffer;
import umc.cdc.hds.auth.AuthFactory;
import umc.cdc.hds.auth.HdfsAuth;
import umc.cdc.hds.core.Api;
import umc.cdc.hds.core.HDS;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.HDSUtil;
import static umc.cdc.hds.core.HDSUtil.loadCustomHdsConfiguration;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.core.ShareableObjectUtils;
import umc.cdc.hds.datastorage.status.ErrorInfo;
import umc.cdc.hds.dblog.HdsLog;
import umc.cdc.hds.dblog.HdsLogHelper;
import umc.cdc.hds.exceptions.ApiNotFoundException;
import umc.cdc.hds.exceptions.AuthException;
import umc.cdc.hds.exceptions.IllegalRequestException;
import umc.cdc.hds.httpserver.ResponseBuilder.Response;
import umc.cdc.hds.task.TaskManager.Task;
import umc.cdc.hds.load.ClusterLoad;
import umc.cdc.hds.load.ClusterLoadImpl;
import umc.cdc.hds.load.NodeLoadInfo;
import umc.cdc.hds.mapping.AccountQuery;
import umc.cdc.hds.mapping.Mapping;
import umc.cdc.hds.requestloadbalancer.Request;
import umc.cdc.hds.requestloadbalancer.Request.Action;
import umc.cdc.hds.requestloadbalancer.RequestLoadBalancer;
import umc.cdc.hds.requestloadbalancer.RequestRedirectPlan;
import umc.cdc.hds.task.KillRedirect;
import umc.cdc.hds.tools.SharedMemory;
import umc.cdc.hds.tools.StopWatch;
import umc.cdc.hds.uri.UriParser;
import umc.cdc.hds.uri.UriRequest;
import umc.cdc.hds.uri.UriRequestExchange;
import umc.udp.core.framework.ShareableObject;
import umc.cdc.hds.tools.ThreadPoolManager;

/**
 * HttpServer for HDS.
 */
public final class HdsHttpServer implements Closeable {

  private static final Log LOG = LogFactory.getLog(HdsHttpServer.class);
  private final Configuration conf;
  private Mapping mapping;
  private HttpServer server;
  private ShareableObject<RequestLoadBalancer> balancer;
  private ShareableObject<ClusterLoad> clusterLoad;
  private ShareableObject<HdfsAuth> auth;
  private ShareableObject<HdsMetrics> metrics;
  private Logger dbLogger;
  private final int longTermTaskMaxNum;
  private final int shortTermTaskMaxNum;
  private final AtomicLong shortTermTaskNum = new AtomicLong(0);
  private final SharedMemory responseBuffer;

  private HdsHttpServer(Configuration conf) {
    this.conf = conf;
    this.shortTermTaskMaxNum = conf.getInt(
            HDSConstants.HTTP_SERVER_SHORT_TERM_TASK_NUM,
            HDSConstants.DEFAULT_HTTP_SERVER_SHORT_TERM_TASK_NUM);
    this.longTermTaskMaxNum = conf.getInt(
            HDSConstants.HTTP_SERVER_LONG_TERM_TASK_NUM,
            HDSConstants.DEFAULT_HTTP_SERVER_LONG_TERM_TASK_NUM);
    this.responseBuffer = new SharedMemory(
            // 5 is for the situation that long and short task are all full
            // that httpserver have to wait for one of them done to response
            // other user. Ex. short-term task is full  or long term task is full.
            shortTermTaskMaxNum + longTermTaskMaxNum + 5,
            conf.getInt(
                    HDSConstants.JSON_BUFFER_LIMIT,
                    HDSConstants.DEFAULT_JSON_BUFFER_LIMIT),
            conf.getLong(
                    HDSConstants.MEMORY_ALLOCATE_TIMEOUT,
                    HDSConstants.DEFAULT_MEMORY_ALLOCATE_TIMEOUT));
    try {
      initializeHdsService(conf);
    } catch (Exception ex) {
      LOG.error("initialize hds error.", ex);
      close();
    }
  }

  public static ShareableObject<HdsHttpServer> getInstance(Configuration conf)
          throws Exception {
    return ShareableObject.<HdsHttpServer>create(
            (Object obj) -> obj instanceof HdsHttpServer,
            () -> new HdsHttpServer(conf));
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = loadCustomHdsConfiguration();
    HdsHttpServer httpServer = new HdsHttpServer(conf);
  }

  @Override
  public void close() {
    // close httpserver.
    if (server != null) {
      ThreadPoolExecutor t = (ThreadPoolExecutor) server.getExecutor();
      t.shutdown();
      server.stop(1);
    }
    // close all daemon
    ThreadPoolManager.shutdownNowAll();
    HDSUtil.closeWithLog(mapping, LOG);
    HDSUtil.closeWithLog(balancer, LOG);
    HDSUtil.closeWithLog(clusterLoad, LOG);
    HDSUtil.closeWithLog(auth, LOG);
    HDSUtil.closeWithLog(dbLogger, LOG);
    HDSUtil.closeWithLog(metrics, LOG);
    TaskManager.close();
    HDS.close();
    HDSUtil.closeWithLog(responseBuffer, LOG);
  }

  private void initializeHdsService(Configuration conf) throws Exception {
    // load should create first, it create zookeeper node when starts
    // and it will affect when others want to get server load or count.
    this.clusterLoad = ClusterLoadImpl.getInstance(conf);
    this.mapping = new Mapping(conf);
    this.balancer = ShareableObjectUtils.createRequestLoadBalancer(conf);
    this.auth = AuthFactory.getHdfsAuth(conf);
    this.dbLogger
            = conf.getBoolean(HDSConstants.LOGGER_ENABLE, HDSConstants.DEFAULT_LOGGER_ENABLE)
            ? HdsLogHelper.getDbLogger(HdsLogHelper.getInfo(), new DbLogConfig(conf))
            : EmptyDbLogger.getDbLogger();
    HDS.initialize(conf);
    // Metrics system must be created after hds all started.
    this.metrics = HdsMetrics.getInstance(this);
    TaskManager.initialize(conf, metrics);
    initializeHttpServer();
  }

  private void initializeHttpServer()
          throws UnknownHostException, IOException {
    final int maxHttpThreadNum = longTermTaskMaxNum + shortTermTaskMaxNum;
    InetSocketAddress inetAddress = createSocketAddress();
    server = HttpServer.create(inetAddress, 0);
    addHandlers(server);
    // We will restrict task num by ourself, set max threadnum to infinite
    // is because we still have to send response to those task that was
    // rejected.
    ThreadPoolExecutor exec = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE, 60L,
            TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    server.setExecutor(exec);
    server.start();
    LOG.info("Server run on " + inetAddress.toString() + " port: "
            + inetAddress.getPort() + " thread num: " + maxHttpThreadNum);
  }

  private void addHandlers(HttpServer server) {
    server.createContext(HDSConstants.HTTP_DEFAULT_ACCESS_URL, new AccessHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_DELETE_URL, new DeleteHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_BATCH_DELETE_URL, new BatchDeleteHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_LIST_URL, new ListHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_LIST_URL2, new ListHandler2());
    server.createContext(HDSConstants.HTTP_DEFAULT_MAPPING_ADD_URL, new AddMappingHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_MAPPING_DELETE_URL, new DeleteMappingHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_MAPPING_LIST_URL, new ListMappingHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_WATCH_URL, new WatchHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_RUN_URL, new DrsRunHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_LOADING_URL, new LoadingHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_KILL_URL, new KillHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_KILL_DRS_URL, new KillDrsHandler());
    server.createContext(HDSConstants.HTTP_DEFAULT_WATCH_DRS_URL, new WatchDrsHandler());
    server.createContext(HDSConstants.HTTP_V2_ACCESS_URL, new AccessHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_DELETE_URL, new DeleteHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_BATCH_DELETE_URL, new BatchDeleteHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_LIST_URL, new ListHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_MAPPING_ADD_URL, new AddMappingHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_MAPPING_DELETE_URL, new DeleteMappingHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_MAPPING_LIST_URL, new ListMappingHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_WATCH_URL, new WatchHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_LOADING_URL, new LoadingHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_KILL_URL, new KillHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_RUN_URL, new DrsRunHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_KILL_DRS_URL, new KillDrsHandlerV2());
    server.createContext(HDSConstants.HTTP_V2_WATCH_DRS_URL, new WatchDrsHandlerV2());
  }

  private InetSocketAddress createSocketAddress() throws UnknownHostException {
    Optional<String> address = Optional.ofNullable(
            conf.get(HDSConstants.HTTP_SERVER_ADDRESS));
    final int port = conf.getInt(HDSConstants.HTTP_SERVER_PORT,
            HDSConstants.DEFAULT_HTTP_SERVER_PORT);
    // default IP Address is 0.0.0.0
    InetAddress inetAddress = address.isPresent()
            ? inetAddress = InetAddress.getByName(address.get())
            : new InetSocketAddress(port).getAddress();
    return new InetSocketAddress(inetAddress, port);
  }

  abstract class Handler implements HttpHandler {

    public abstract Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog);

    public abstract String getContext();

    public abstract boolean isLongTermTask();

    public Handler() {

    }

    @Override
    public void handle(HttpExchange t) {

      boolean isAsync = false;

      HdsLog hdsLog = new HdsLog();
      hdsLog.setRequestStartTime(System.currentTimeMillis());
      try {
        hdsLog.setHost(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException ex) {
        hdsLog.setHost("localhost");
      }
      ByteBuffer buffer = null;
      try {
        final HttpUriParser httpParser = new HttpUriParser(t.getRequestURI());
        isAsync = httpParser.getAsync();
        final Exchange exchange = new Exchange(t);
        buffer = responseBuffer.take();
        try (Response response = createResponse(httpParser, exchange, buffer, hdsLog)) {
          if (exchange.isSend()) {
            return;
          }
          StopWatch responseTime = new StopWatch();

          /* >>> 2021/10/29 Brad added for CORS support >>> */
          Boolean allowCORS = conf.getBoolean(HDSConstants.CORS_ALLOW, HDSConstants.DEFAULT_CORS_ALLOW);
          if (allowCORS) {
            String allowOrigins = conf.get(HDSConstants.CORS_ORIGINS, HDSConstants.DEFAULT_CORS_ORIGINS);
            if (allowOrigins.equals("*"))
              exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            else
              LOG.error("[CORS] Currently, \"access-control-allow-origin\" only supports \"*\"");
            if (exchange.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
              exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "*");
              exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "*");
              exchange.sendResponseHeaders(204, -1);
            }
          }
          /* <<< 2021/10/29 Brad added for CORS support <<< */

          response.send();
          hdsLog.setResponseElapsedTime(responseTime.getElapsed());
        }
      } catch (IOException ex) {
        LOG.error(ex);
      } finally {
        if (buffer != null) {
          responseBuffer.restore(buffer);
        }
        // no matter client close conn or not, we still log this record.
        if (isAsync) {
          hdsLog.countDown();
        } else if (dbLogger != null) {
          dbLogger.addLog(HdsLogHelper.toLogCell(hdsLog));
        }
        if (!isLongTermTask()) {
          shortTermTaskNum.decrementAndGet();
        }
        t.close();
      }
    }

    public Response createResponse(
            HttpUriParser httpParser, HttpExchange t, ByteBuffer buffer, HdsLog hdsLog)
            throws IOException {
      ResponseBuilder reb = new ResponseBuilder()
              .setBuffer(buffer.array())
              .setExchange(new Exchange(t))
              .setFormat(ResponseBuilder.Format.json);
      try {
        checkApiExists(t);
        restrictTasks(t.getRequestURI().getPath());
        restrictAuth(httpParser);
        Result result = action(httpParser, t, hdsLog);
        return reb
                .setContent(result)
                .build();
      } catch (ApiNotFoundException apiEx) {
        return reb
                .setContent(new Result(
                        Status.NOT_FOUND,
                        new ErrorInfo(apiEx),
                        HDSConstants.ERROR_RESPONSE))
                .build();
      } catch (AuthException ex) {
        return reb
                .setContent(new Result(
                        Status.UNAUTHORIZED,
                        new ErrorInfo(ex),
                        HDSConstants.ERROR_RESPONSE))
                .build();
      } catch (Exception ex) {
        return reb
                .setContent(new Result(
                        Status.INTERNAL_SERVER_ERROR,
                        new ErrorInfo(ex),
                        HDSConstants.ERROR_RESPONSE))
                .build();
      }
    }

    private void checkApiExists(HttpExchange t) throws ApiNotFoundException {
      if (!getContext().equals(t.getRequestURI().getPath())) {
        throw new ApiNotFoundException("No such API");
      }
    }

    // loading should not in short term task, since long term task will call
    // short term task. If we restrict it, long term will fail because the
    // limit number of short term task.
    private synchronized void restrictTasks(String context) throws IOException {
      if (!isLongTermTask()
              && shortTermTaskNum.incrementAndGet() > shortTermTaskMaxNum) {
        throw new IOException("Short Term Task is full now.");
      }
    }

    private void restrictAuth(HttpUriParser httpParser)
            throws AuthException {
      Api api = Api.find(getContext().split("/")[3]).get();
      Optional<String> token = httpParser.getToken();
      if (!auth.get().access(token.orElse(""), api)) {
        throw new AuthException("Unauthorized: "
                + "Access is denied due to invalid token.");
      }
    }

    Result redirectV2ToV1(HttpUriParser httpParser, HttpExchange exchange) {
      try {
        URIBuilder url = new URIBuilder(httpParser.toUri());

        if (httpParser.getFrom().isPresent() && httpParser.getFrom().get().startsWith("hds")) {
          if (httpParser.getFrom().get().contains("?")) {
            url.setParameter(HDSConstants.URL_ARG_FROM, httpParser.getFrom().get().concat("&ver=v2"));
          } else {
            url.setParameter(HDSConstants.URL_ARG_FROM, httpParser.getFrom().get().concat("?ver=v2"));
          }
        }
        if (httpParser.getTo().isPresent() && httpParser.getTo().get().startsWith("hds")) {
          if (httpParser.getTo().get().contains("?")) {
            url.setParameter(HDSConstants.URL_ARG_TO, httpParser.getTo().get().concat("&ver=v2"));
          } else {
            url.setParameter(HDSConstants.URL_ARG_TO, httpParser.getTo().get().concat("?ver=v2"));
          }
        }

        String urlString = url.build().toString().replaceFirst(
                "/dataservice/" + HDSConstants.HDS_VERSION_V2,
                "/dataservice/" + HDSConstants.HDS_VERSION);

        String redirectUrl = new StringBuilder()
                .append("http://")
                .append(exchange.getLocalAddress().getHostName())
                .append(":")
                .append(exchange.getLocalAddress().getPort())
                .append(urlString)
                .toString();
        RedirectionException redirEx = new RedirectionException(
                "hds ver2", new URL(redirectUrl),
                exchange.getRequestMethod());
        exchange.getResponseHeaders().add(
                "Location",
                redirEx.getRedirectUrl().toString());
        Status code = redirEx.getMethod().equals("POST")
                ? Status.TEMPORARY_REDIRECT
                : Status.SEE_OTHER;
        return new Result(
                code,
                new ErrorInfo(redirEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ioEx) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ioEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (Exception ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }
  }

  class ListHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder(HDSConstants.API_LIST)
                .append(" - From: ")
                .append(httpParser.getFrom().orElse(null)).toString());

        hdsLog.setApi(HDSConstants.API_LIST);
        hdsLog.setInput(httpParser.getFrom().orElse(null));

        //<-- show jdbc table   edit by chuan 20211102
        String from_Uri = httpParser.getFrom().toString();
        String table_Uri = httpParser.getJdbcTable().toString();

        if (from_Uri.contains("jdbc")){
          LOG.info("Execute show tables or column name in table.");
          String JdbcInfo= httpParser.getJdbcInfo().orElseThrow(
                  () -> new IllegalRequestException("Please specify < info= >."));
          JdbcInfo = JdbcInfo.replace("%3",":");
          LOG.info("JdbcInfo = "+JdbcInfo);

          // if table name have been specify, execute show column.
          String table= table_Uri.substring( 9, table_Uri.length()-1 );
          if (!table.equals("empt")){
            String tableName = executeGetTables(JdbcInfo);
            LOG.info("Execute show columns in table : "+ table);
            String columnName = executeGetCloumnName(JdbcInfo,table);
            /*Result result = new Result(Status.OK, tableName +"column name :" + columnName,
                    HDSConstants.LIST_DATAINFO);*/
            Result result = new Result(Status.OK, new DatabaseInfo(tableName +"column name :" + columnName),
                    HDSConstants.LIST_DATAINFO);
            return result;
          }

          String tableName = executeGetTables(JdbcInfo);
          Result result = new Result(Status.OK, new DatabaseInfo(tableName),
                  HDSConstants.LIST_DATAINFO);
          return result;
        }
        //--> show jdbc table / column

        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        final String fromUri = httpParser.getFrom().orElseThrow(
                () -> new IllegalRequestException("Please specify <from_uri_20180809>."));

        UriRequest from = UriParser.valueOf(fromUri, mapping);
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        Result result = new Result(Status.OK, HDS.list(from, httpParser.getQuery(), hdsLog),
                HDSConstants.LIST_DATAINFO);
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }
    public String executeGetTables(String info) throws JdbcDataStorageException {
        Connection con = null;
        try {
          LOG.info("Connecting to JDBC");
          con = DriverManager.getConnection(info);
          LOG.info("Success");
          LOG.info("Getting table name");
          DatabaseMetaData dbmd = con.getMetaData();
          ResultSet rs = dbmd.getTables(null, null, "%", null);
          // Display the result set.
          String t = "";
          while ( rs.next() ) {
            LOG.info("table name = " + rs.getString(3));
            t = t + rs.getString(3) + ", "; //table name is in column 3
          }
          t = t.substring( 0, t.length() - 2 );
          rs.close();
          LOG.info("All Table name :"+t);
          return t;
        } catch (SQLException e) {
          LOG.error("Connect failed");
          throw new JdbcDataStorageException("JDBC connect failed." + e);
        }
    }
    public String executeGetCloumnName(String info, String table) throws JdbcDataStorageException {
      Connection con = null;
      try {
        LOG.info("Connecting to JDBC");
        con = DriverManager.getConnection(info);
        System.out.println("Success");
        LOG.info("Getting column name");

        DatabaseMetaData dbMetaData = con.getMetaData();
        ResultSet rs = dbMetaData.getColumns(null, null, table, null);
        LOG.info("List of column names in the current table ");
        //Retrieving the list of column names
        String t = "";
        while (rs.next()) {
          t = t + rs.getString("COLUMN_NAME") + ", ";
        }
        t = t.substring( 0, t.length() - 2 );
        rs.close();
        LOG.info("All column name :" + t);
        return t;
      } catch (SQLException e) {
        LOG.error("Connect failed");
        throw new JdbcDataStorageException("JDBC connect failed." + e);
      }
    }
    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_LIST_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class ListHandler2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder(HDSConstants.API_LIST2)
                .append(" - From: ")
                .append(httpParser.getFrom().orElse(null)).toString());
        hdsLog.setApi(HDSConstants.API_LIST2);
        hdsLog.setInput(httpParser.getFrom().orElse(null));
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        final String fromUri = httpParser.getFrom().orElseThrow(
                () -> new IllegalRequestException("Please specify <from_uriL2>."));
        UriRequest from = UriParser.valueOf(fromUri, mapping);
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        Result result = new Result(Status.OK, HDS.list(from, httpParser.getQuery(), hdsLog),
                HDSConstants.LIST_DATAINFO);
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_LIST_URL2;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class ListHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_LIST_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class DeleteHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder(HDSConstants.API_DELETE)
                .append(" - From: ")
                .append(httpParser.getFrom().orElse(null)).toString());
        hdsLog.setApi(HDSConstants.API_DELETE);
        hdsLog.setInput(httpParser.getFrom().orElse(null));
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        final String fromUri = httpParser.getFrom().orElseThrow(
                () -> new IllegalRequestException("Please specify <from_uri>."));
        UriRequest from = UriParser.valueOf(fromUri, mapping);
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        Result result = new Result(
                Status.OK,
                HDS.delete(from, hdsLog, httpParser.getRecursive()),
                HDSConstants.LIST_DATAINFO);
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_DELETE_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class DeleteHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_DELETE_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class BatchDeleteHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder(HDSConstants.API_BATCHDELETE)
                .append(" - From: ")
                .append(httpParser.getFrom().orElse(null)).toString());
        hdsLog.setApi(HDSConstants.API_BATCHDELETE);
        hdsLog.setInput(httpParser.getFrom().orElse(null));
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        final String fromUri = httpParser.getFrom().orElseThrow(
                () -> new IllegalRequestException("Please specify <from_uri>."));
        final boolean ifWildcard = httpParser.getEnableWildcard();
        UriRequest from = UriParser.valueOf(fromUri, mapping);
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        Result result = new Result(
                Status.OK,
                HDS.batchDelete(from, ifWildcard, hdsLog),
                HDSConstants.LIST_DATAINFO);
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_BATCH_DELETE_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class BatchDeleteHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_BATCH_DELETE_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class AccessHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_ACCESS)
                .append(" - From: ").append(httpParser.getFrom().orElse(null))
                .append(", To: ").append(httpParser.getTo().orElse(null)).toString());
        hdsLog.setApi(HDSConstants.API_ACCESS);
        hdsLog.setInput(httpParser.getFrom().orElse(null));
        hdsLog.setOutput(httpParser.getTo().orElse(null));

        String fromUri = httpParser.getFrom().orElseThrow(
                () -> new IllegalRequestException(
                        "Please specify <from_uri>."));

        String toUri = httpParser.getTo().orElseThrow(
                () -> new IllegalRequestException(
                        "Please specify <to_uri>."));
        //String JdbcInfo;
        String JdbcFile;
        String JdbcQuery;
        String JdbcFormat;
        String JdbcHeader;

        // overwrite froUri in jdbc case --->
        if(fromUri.contains("jdbc:///")){
          String JdbcInfo= httpParser.getJdbcInfo().orElseThrow(
                  () -> new IllegalRequestException("Please specify < info= >."));
          //check if query is from file
          if (!httpParser.getFile().isPresent() && !httpParser.getJdbcQuery().isPresent()) {
            throw new IllegalRequestException("Need to specify < query= > or < file= >.");
          }else if (httpParser.getFile().isPresent() && !httpParser.getJdbcQuery().isPresent()){
            JdbcFile = httpParser.getFile().get();
            fromUri = fromUri + "?" + "info=" + JdbcInfo + "&" + "file=" + JdbcFile;
          }else if (!httpParser.getFile().isPresent() && httpParser.getJdbcQuery().isPresent()){
            JdbcQuery = httpParser.getJdbcQuery().get();
            fromUri = fromUri + "?" + "info=" + JdbcInfo + "&" + "query=" + JdbcQuery;
          }else{
            JdbcQuery = httpParser.getJdbcQuery().get();
            fromUri = fromUri + "?" + "info=" + JdbcInfo + "&" + "query=" + JdbcQuery;
          }
          if (httpParser.getJdbcFormat().isPresent()){
            JdbcFormat = httpParser.getJdbcFormat().get();
            fromUri = fromUri + "&" + "format=" + JdbcFormat;
          }
          if (httpParser.getJdbcHeader().isPresent()){
            JdbcHeader = httpParser.getJdbcHeader().get();
            fromUri = fromUri + "&" + "header=" + JdbcHeader;
          }
          //LOG.info("@@@@@@@@@@@@@@@@@@"+fromUri);
        }
        // <--- overwrite froUri in jdbc case

        UriRequestExchange reqFrom = new UriRequestExchange(
                UriParser.valueOf(fromUri, mapping),
                new Exchange(exchange)),
                reqTo = new UriRequestExchange(
                        UriParser.valueOf(toUri, mapping),
                        new Exchange(exchange));
        checkRequestMethod(exchange, reqFrom.getProtocol());
        boolean isAsync = httpParser.getAsync();
        Optional<String> redirect = httpParser.getRedirectorFrom();

        restrictAsync(isAsync, reqFrom, reqTo);
        if (!httpParser.getRedirectorFrom().isPresent() && !restrictRedirect(reqFrom, reqTo)) {
          Request request = new Request(ServerName.valueOf(
                  exchange.getLocalAddress().getHostName(),
                  exchange.getLocalAddress().getPort(), 0),
                  Action.ACCESS);
          RequestRedirectPlan plan = balancer.get().balanceRequest(request);
          if (!plan.getBestRedirectServer().equals(request.getServerName())) {
            String redirectUrl = new StringBuilder()
                    .append("http://")
                    .append(plan.getBestRedirectServer().getHostAndPort())
                    .append(httpParser.toUri())
                    .append("&")
                    .append(HDSConstants.URL_REDIRECT_FROM)
                    .append("=").append(exchange.getLocalAddress().getHostName())
                    .toString();
            LOG.info("Redirect: " + redirectUrl);
            throw new RedirectionException(
                    "load balance", new URL(redirectUrl),
                    exchange.getRequestMethod());
          }
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());

        StopWatch action = new StopWatch();

        Task task = TaskManager.createTask(
                reqFrom, reqTo, redirect, isAsync, hdsLog);
        task.schedule();
        task.execute();
        Result result = task.lastStep();

        hdsLog.setActionElapsedTime(action.getElapsed());

        return result;
      } catch (IllegalRequestException illegalEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (RedirectionException redirEx) {
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        hdsLog.setApi(HDSConstants.API_ACCESS + "_REDIRECT");
        exchange.getResponseHeaders().add(
                "Location",
                redirEx.getRedirectUrl().toString());
        Status code = redirEx.getMethod().equals("POST")
                ? Status.TEMPORARY_REDIRECT
                : Status.SEE_OTHER;
        return new Result(
                code,
                new ErrorInfo(redirEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ioEx) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ioEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (Exception ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    // We should not let any file protocol request redirect, or it will
    // be send to other server.
    private boolean restrictRedirect(UriRequest from, UriRequest to) {
      if (from.getProtocol().equals(Protocol.file)
              || to.getProtocol().equals(Protocol.file)) {
        return true;
      } else {
        return false;
      }
    }

    private void restrictAsync(
            boolean isAsync, UriRequest reqFrom, UriRequest reqTo)
            throws IOException {
      if (isAsync
              && (reqFrom.getProtocol().equals(Protocol.local)
              || reqFrom.getProtocol().equals(Protocol.local))) {
        throw new IOException(
                "Async mode is not allowed when protocol is local.");
      }
    }

    /**
     * Check if the Request Method is correct.
     *
     * @param exchange
     * @param from
     * @throws IllegalRequestException
     */
    private void checkRequestMethod(HttpExchange exchange, Protocol from)
            throws IllegalRequestException {
      switch (from) {
        case local:
          if (!exchange.getRequestMethod().equals("POST")) {
            throw new IllegalRequestException("Request method should be POST.");
          }
          break;
        default:
          if (!exchange.getRequestMethod().equals("GET")) {
            throw new IllegalRequestException("Request method should be GET.");
          }
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_ACCESS_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return true;
    }

  }

  class AccessHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_ACCESS_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class KillHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_KILL)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_KILL);

        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        final String id = httpParser.getId().orElseThrow(()
                -> new IllegalRequestException("Please specify id."));

        int port = conf.getInt(
                HDSConstants.HTTP_SERVER_PORT,
                HDSConstants.DEFAULT_HTTP_SERVER_PORT);
        String host = conf.get(
                HDSConstants.HTTP_SERVER_ADDRESS,
                InetAddress.getLocalHost().getHostName());
        ServerName serverName = ServerName.valueOf(host, port, 0);
        Request request = new Request(serverName, Action.KILL);
        if (!httpParser.getRedirectorFrom().isPresent()) {
          KillRedirect parser = new KillRedirect(Optional.of(id), serverName);
          RequestRedirectPlan plan = parser.getRedirectPlan();
          if (!plan.getBestRedirectServer().equals(request.getServerName())) {
            String redirectUrl = new StringBuilder()
                    .append("http://")
                    .append(plan.getBestRedirectServer().getHostAndPort())
                    .append(httpParser.toUri())
                    .append("&")
                    .append(HDSConstants.URL_REDIRECT_FROM)
                    .append("=").append(exchange.getLocalAddress().getHostName())
                    .toString();
            LOG.info("Redirect: " + redirectUrl);
            throw new RedirectionException(
                    "load balance", new URL(redirectUrl),
                    exchange.getRequestMethod());
          }
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                TaskManager.abortTask(id),
                HDSConstants.TASK);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (RedirectionException redirEx) {
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        hdsLog.setApi(HDSConstants.API_KILL + "_REDIRECT");
        exchange.getResponseHeaders().add(
                "Location",
                redirEx.getRedirectUrl().toString());
        return new Result(
                Status.SEE_OTHER,
                new ErrorInfo(redirEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (Exception ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_KILL_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class KillHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_KILL_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class AddMappingHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_ADDMAPPING)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_ADDMAPPING);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                mapping.add(new AccountQuery(
                        httpParser.getQuery().getQueries())),
                HDSConstants.MAPPING_ACCOUNT);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_MAPPING_ADD_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class AddMappingHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_MAPPING_ADD_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class DeleteMappingHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_DELETEMAPPING)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_DELETEMAPPING);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        if (!checkReqHasDeleteMappingArgs(httpParser)) {
          throw new IllegalRequestException(HDSConstants.INCORRECT_DELETE_MAPPING_QUERY);
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                mapping.delete(httpParser.getQuery()),
                HDSConstants.MAPPING_ACCOUNT);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_MAPPING_DELETE_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

    private boolean checkReqHasDeleteMappingArgs(HttpUriParser httpParser) {
      for (String arg : HDSConstants.DELETE_MAPPING_LEGAL_ARGS) {
        if (httpParser.getQuery().contains(arg)) {
          return true;
        }
      }
      return false;
    }
  }

  class DeleteMappingHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_MAPPING_DELETE_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class ListMappingHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_LISTMAPPING)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_LISTMAPPING);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                mapping.list(httpParser.getQuery()),
                HDSConstants.MAPPING_ACCOUNT);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_MAPPING_LIST_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class ListMappingHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_MAPPING_LIST_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class WatchHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_WATCH)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_WATCH);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request method should be GET.");
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                TaskManager.getTaskInfo(httpParser.getQuery()),
                HDSConstants.TASK);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (Exception ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_WATCH_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class WatchHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_WATCH_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class DrsRunHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_RUN)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_RUN);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request nethod should be GET.");
        }
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                runDrsApp(httpParser),
                HDSConstants.RUN_RESPONSE_TITLE);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalReqEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalReqEx),
                HDSConstants.ERROR_RESPONSE);
      } catch (IOException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    private ServiceStatus runDrsApp(HttpUriParser httpParser)
            throws IOException {
      DRSClient client = new DRSClient();
      client.setHdsAccessAddress(conf.get("hds.access.address"));
      client.setDrsConfigLocation(conf.get("drs.config.location"));
      ServiceStatus serviceStatus;
      try {
        client.init(httpParser.getCode().orElse(null),
                httpParser.getData().orElse(null),
                httpParser.getConfig().orElse(null),
                httpParser.getCodeOut().orElse(null),
                httpParser.getCopyTo().orElse(null),
                httpParser.getConsoleTo().orElse(null));
        if (httpParser.getAsync()) {
          client.asyncStart();
        } else {
          client.start();
          // wait for drs app done.
          while (!client.isAbort()) {
            Thread.sleep(1000);
          }
        }
        serviceStatus = client.getServiceStatus();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      } finally {
        if (!httpParser.getAsync()) {
          client.close();
        }
      }
      return serviceStatus;
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_RUN_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class DrsRunHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_RUN_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class KillDrsHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_KILL_DRS)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_KILL_DRS);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request nethod should be GET.");
        }
        String id = httpParser.getApplicationId().orElse("");
        try (DRSClient client = new DRSClient()) {
          client.setHdsAccessAddress(conf.get("hds.access.address"));
          client.setDrsConfigLocation(conf.get("drs.config.location"));
          client.setKillEnviroment();
          KilldrsResponseMessage res = client.kill(id);
          hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
          StopWatch action = new StopWatch();
          Result result = new Result(
                  Status.OK,
                  res,
                  HDSConstants.RUN_RESPONSE_TITLE
          );
          hdsLog.setActionElapsedTime(action.getElapsed());
          return result;
        }
      } catch (IllegalRequestException ex) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      } catch (DrsClientException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      } catch (Exception ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_KILL_DRS_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class KillDrsHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_KILL_DRS_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class WatchDrsHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_WATCH_DRS)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_WATCH_DRS);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException("Request nethod should be GET.");
        }
        Optional<String> id = httpParser.getApplicationId();
        boolean history = httpParser.getHistory();
        int limit = httpParser.getQuery().getQueryValueAsInteger(
                HDSConstants.LIMIT, -1);
        DRSClient client = new DRSClient();
        client.setHdsAccessAddress(conf.get("hds.access.address"));
        client.setDrsConfigLocation(conf.get("drs.config.location"));
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result;
        if (history) {
          result = new Result(
                  Status.OK,
                  client.watchHistory(limit).iterator(),
                  HDSConstants.WATCHDRS_TITLE
          );
        }
        if (id.isPresent()) {
          ServiceStatus serviceStatus = client.watch(id.get());
          result = new Result(
                  Status.OK,
                  serviceStatus,
                  HDSConstants.WATCHDRS_TITLE
          );
        } else {
          result = new Result(
                  Status.OK,
                  client.watchAll().iterator(),
                  HDSConstants.WATCHDRS_TITLE
          );
        }
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (ApplicationIdNotFoundException | DrsClientException ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      } catch (Exception ex) {
        return new Result(
                Status.INTERNAL_SERVER_ERROR,
                new ErrorInfo(ex),
                HDSConstants.ERROR_RESPONSE);
      }

    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_WATCH_DRS_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class WatchDrsHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_WATCH_DRS_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  class LoadingHandler extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      try {
        LOG.info(new StringBuilder().append(HDSConstants.API_LOADING)
                .append(" - ").append(httpParser.toUri()));
        hdsLog.setApi(HDSConstants.API_LOADING);
        if (!exchange.getRequestMethod().equals("GET")) {
          throw new IllegalRequestException(
                  "Request nethod should be GET.");
        }
        Iterator<NodeLoadInfo> nodeLoadInfo
                = clusterLoad.get().getClusterLoading();
        hdsLog.setRequestInitElapsedTime(System.currentTimeMillis() - hdsLog.getRequestStartTime());
        StopWatch action = new StopWatch();
        Result result = new Result(
                Status.OK,
                nodeLoadInfo,
                HDSConstants.LOADING);
        hdsLog.setActionElapsedTime(action.getElapsed());
        return result;
      } catch (IllegalRequestException illegalEx) {
        return new Result(
                Status.FORBIDDEN,
                new ErrorInfo(illegalEx),
                HDSConstants.ERROR_RESPONSE);
      }
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_DEFAULT_LOADING_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }

  }

  class LoadingHandlerV2 extends Handler {

    @Override
    public Result action(HttpUriParser httpParser, HttpExchange exchange, HdsLog hdsLog) {
      return redirectV2ToV1(httpParser, exchange);
    }

    @Override
    public String getContext() {
      return HDSConstants.HTTP_V2_LOADING_URL;
    }

    @Override
    public boolean isLongTermTask() {
      return false;
    }
  }

  public long getLongTaskNum() {
    return TaskManager.getTaskNum();
  }

  public long getShortTaskNum() {
    return shortTermTaskNum.get();
  }

  public long getJdbcConnNum() {
    return HDS.getDataStorageConnNum(Protocol.jdbc);
  }

  public long getFtpConnNum() {
    return HDS.getDataStorageConnNum(Protocol.ftp);
  }

  public long getSmbConnNum() {
    return HDS.getDataStorageConnNum(Protocol.smb);
  }

  public Configuration getConfig() {
    return conf;
  }

}
