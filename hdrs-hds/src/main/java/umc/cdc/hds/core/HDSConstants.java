package umc.cdc.hds.core;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class HDSConstants {

  public static final String NO_SUPPORTED_METHOD = "Query not supported.";
  public static final String NO_PROTOCOL = "Protocol not supported.";
  public static final String RECOVERY_FAILED = "Recovery file failed";
  public static final String CHANGE_TO_FILE_FAILED = "Change tmp file failed";
  public static final String SET_FILE_TIME_FAILED = "Set file last modified time failed";
  public static final String REMOVE_FILE_FAILED = "Remove origin file failed";
  public static final String REMOVE_TMP_FAILED = "Remove temp file failed";
  public static final String LOGIN_FAILED = "Login failed";

  public static final String WRONG_PROTOCOL = "Wrong protocol";
  public static final String WRONG_URI = "Incorrect URI format.";
  public static final String NO_HOST = "Cannot find host";
  public static final String NO_SCHEMA = "Cannot get schema";
  public static final String NO_PATH = "Cannot get path";
  public static final String NO_DIR = "Cannot find parent directory";
  public static final String NO_NAME = "Cannot get name";
  public static final String NO_FILE = "No such file/directory";

  public static final String INCORRECT_SCHEME = "Wrong URI scheme.";
  public static final String INCORRECT_MAPPING = "Wrong mapping.";

  public static final String INCORRECT_LIST_FILE = "list path should be dirctory";
  public static final String INCORRECT_DELETE_FILE = "delete path should be file";
  public static final String INCORRECT_BATCHDELETE_QUERY = "batchdelete should set query";
  public static final String INCORRECT_DELETE_MAPPING_QUERY = "deletemapping should set query";

  public static final String LIST_ELEMENT_MISSING_ARG = "Miss some ListElement arguement.";
  public static final String URL_MISSING_ARG = "Miss some URL arguement.";

  public static final String ERROR_NO_MAPPING = "Mapping not found.";

  // URI args
  public static final String COMMON_URI_ARG_KEEP = "keep";
  public static final String URL_ARG_FROM = "from";
  public static final String URL_ARG_TO = "to";
  public static final String URL_ENABLE_WILDCARD = "enablewildcard";
  public static final String URL_REDIRECT_FROM = "redirectfrom";
  public static final String URL_ARG_TOKEN = "token";
  public static final String URL_ARG_DELETE_RECURSIVE = "recursive";

  public static final String URL_ARG_ORDERBY = "orderby";
  public static final String URL_ARG_ASC = "asc";
  public static final String URL_ARG_HISTORY = "history";

  public static final String MAPPING_ACCOUNT = "account";
  public static final String MAPPING_ARG_ID = "id";
  public static final String MAPPING_ARG_HOST = "host";
  public static final String MAPPING_ARG_PORT = "port";
  public static final String MAPPING_ARG_MINPORT = "minport";
  public static final String MAPPING_ARG_MAXPORT = "maxport";
  public static final String MAPPING_ARG_USER = "user";
  public static final String MAPPING_ARG_DOMAIN = "domain";
  public static final String MAPPING_ARG_PASSWD = "password";
  public static final String MAPPING_ARG_OFFSET = "offset";
  public static final String MAPPING_ARG_LIMIT = "limit";

  public static final String HDS_URI_ARG_CLONE = "clone";
  public static final String HDS_URI_ARG_KEY = "key";
  public static final String HDS_URI_ARG_MINKEY = "minkey";
  public static final String HDS_URI_ARG_MAXKEY = "maxkey";
  public static final String HDS_URI_ARG_TIME = "time";
  public static final String HDS_URI_ARG_MINTIME = "mintime";
  public static final String HDS_URI_ARG_MAXTIME = "maxtime";
  public static final String HDS_URI_ARG_SIZE = "size";
  public static final String HDS_URI_ARG_MINSIZE = "minsize";
  public static final String HDS_URI_ARG_MAXSIZE = "maxsize";
  public static final String HDS_URI_ARG_NAME = "name";
  public static final String HDS_URI_ARG_LOCATION = "location";
  public static final String HDS_URI_ARG_VERSION = "ver";
  public static final String OFFSET = "offset";
  public static final String LIMIT = "limit";

  public static final String TASK = "task";
  public static final String TASK_ID = "id";
  public static final String TASK_REDIRECT_FROM = "redirectfrom";
  public static final String TASK_SERVER_NAME = "servername";
  public static final String TASK_CLIENT_NAME = "clientname";
  public static final String TASK_FROM = "from";
  public static final String TASK_TO = "to";
  public static final String TASK_STATE = "state";
  public static final String TASK_PROGRESS = "progress";
  public static final String TASK_MIN_PROGRESS = "minprogress";
  public static final String TASK_MAX_PROGRESS = "maxprogress";
  public static final String TASK_START_TIME = "startTime";
  public static final String TASK_MIN_START_TIME = "minstarttime";
  public static final String TASK_MAX_START_TIME = "maxstarttime";
  public static final String TASK_ELAPSED = "elapsed";
  public static final String TASK_MIN_ELAPSED = "minelapsed";
  public static final String TASK_MAX_ELAPSED = "maxelapsed";
  public static final String TASK_EXPECTED_SIZE = "expectedsize";
  public static final String TASK_MIN_EXPECTED_SIZE = "minexpectedsize";
  public static final String TASK_MAX_EXPECTED_SIZE = "maxexpectedsize";
  public static final String TASK_TRANSFERRED_SIZE = "transferredsize";
  public static final String TASK_MIN_TRANSFERRED_SIZE = "mintransferredsize";
  public static final String TASK_MAX_TRANSFERRED_SIZE = "maxtransferredsize";
  public static final String TASK_HISTORY = "history";

  public static final String RUN_ARG_CODE = "code";
  public static final String RUN_ARG_DATA = "data";
  public static final String RUN_ARG_CONFIG = "config";
  public static final String RUN_ARG_CODEOUT = "codeout";
  public static final String RUN_ARG_COPYTO = "copyto";
  public static final String RUN_ARG_CONSOLETO = "consoleto";

  public static final String RUN_RESPONSE_TITLE = "result";
  public static final String RUN_RESPONSE_ID = "applicationID";
  public static final String RUN_RESPONSE_AMNODE = "AMNode";
  public static final String RUN_RESPONSE_CODE = "Code";
  public static final String RUN_RESPONSE_DATA = "Data";
  public static final String RUN_RESPONSE_CONFIG = "Config";
  public static final String RUN_RESPONSE_COPYTO = "Copyto";
  public static final String RUN_RESPONSE_IS_COMPLETE = "isComplete";
  public static final String RUN_RESPONSE_FILECOUNT = "fileCount";
  public static final String RUN_RESPONSE_HDSFILEINFOCOLLECTTIMESMS
      = "hdsFileInfoCollectTimeMs";
  public static final String RUN_RESPONSE_STATUS = "status";
  public static final String RUN_RESPONSE_ERROR_MESSAGE = "errorMessage";
  public static final String RUN_RESPONSE_PROGRESS = "progress";
  public static final String RUN_RESPONSE_STARTTIME = "startTime";
  public static final String RUN_RESPONSE_ELAPSED = "elapsed";
  public static final String RUN_RESPONSE_NODESSTATUS = "nodesStatus";
  public static final String RUN_RESPONSE_CONTAINER_INFO = "containerInfo";
  public static final String CONTAINER_INFO_NAME = "name";
  public static final String CONTAINER_INFO_ISCLOSE = "isClose";
  public static final String CONTAINER_INFO_NODENAME = "nodeName";
  public static final String CONTAINER_INFO_CONTAINERID = "containerID";
  public static final String CONTAINER_INFO_WORKINGTASK = "workingTask";
  public static final String CONTAINER_INFO_STARTTIME = "startTime";
  public static final String CONTAINER_INFO_ENDTIME = "endTime";
  public static final String CONTAINER_INFO_STATUS = "status";
  public static final String CONTAINER_INFO_NODEDONETASK = "nodeDoneTask";
  public static final String CONTAINER_INFO_NODEFAILTASK = "nodeFailTask";
  public static final String CONTAINER_INFO_CONTAINERINTERVAL = "containerInterval";
  public static final String CONTAINER_INFO_MEMSIZE = "memorySize";
  public static final String RUN_RESPONSE_TASK = "task";
  public static final String TASK_URL = "url";
  public static final String TASK_SIZE = "size(byte)";
  public static final String TASK_STATUS = "taskStatus";
  public static final String LOADING = "loading";
  public static final String NODE_LOAD_HOST = "host";
  public static final String NODE_LOAD_PORT = "port";
  public static final String NODE_LOAD_OPERATION = "operation";
  public static final String LOAD_METHOD = "method";
  public static final String LOAD_DEAL_COUNT = "dealCount";
  public static final String LOAD_DEAL_BYTES = "dealBytes";
  public static final String LOAD_PAST_COUNT = "pastCount";
  public static final String LOAD_PAST_BYTES = "pastBytes";

  public static final String WATCHDRS_TITLE = "applications";
  public static final String APPLICATION_ID = RUN_RESPONSE_ID;
  public static final String APPLICATION_AMNODE = RUN_RESPONSE_AMNODE;
  public static final String APPLICATION_STATUS = RUN_RESPONSE_STATUS;
  public static final String APPLICATION_CODE = RUN_RESPONSE_CODE;
  public static final String APPLICATION_DATA = RUN_RESPONSE_DATA;
  public static final String APPLICATION_CONFIG = RUN_RESPONSE_CONFIG;
  public static final String APPLICATION_COPYTO = RUN_RESPONSE_COPYTO;
  public static final String APPLICATION_FILECOUNT = RUN_RESPONSE_FILECOUNT;
  public static final String APPLICATION_PROGRESS = RUN_RESPONSE_PROGRESS;
  public static final String APPLICATION_ELAPSED = RUN_RESPONSE_ELAPSED;
  public static final String APPLICATION_USED_CONTAINERS = "UsedContainers";
  public static final String APPLICATION_USED_VIRTUAL_MEMORY = "UsedVirtualMemory";
  public static final String APPLICATION_USED_VIRTUAL_CORES = "UsedVirtualCores";

  public static final String DS_URI_ARG_FILE = "file";
  public static final String DS_URI_ARG_DIRECTORY = "directory";
  public static final String HDS_CONFIG_LOCATION = "hds.config.location";
  public static final String LOG4J_CONFIG_FILENAME = "log4j.properties";
  public static final String JCIFS_CONFIG_FILENAME = "jcifs.prp";
  public static final String HDS_CUSTOM_CONFIG_FILENAME = "hds.xml";
  public static final String LARGE_DATA_THRESHOLD
      = "hds.large.data.threshold";
  public static final int DEFAULT_LARGE_DATA_THRESHOLD
      = 10485760;
  public static final String MEMORY_BUFFER_UPPER_LIMIT
      = "hds.memory.buffer.upper.limit";
  public static final int DEFAULT_MEMORY_BUFFER_UPPER_LIMIT
      = DEFAULT_LARGE_DATA_THRESHOLD * 2;
  public static final String MEMORY_ALLOCATE_TIMEOUT
      = "hds.memory.allocate.timeout";
  public static final long DEFAULT_MEMORY_ALLOCATE_TIMEOUT
      = 60000;
  public static final String HDS_PATH
      = "hds.root.path";
  public static final String DEFAULT_HDS_PATH
      = "/hds";
  public static final String HDS_DIR_NUM
      = "hds.dir.num";
  public static final int DEFAULT_HDS_DIR_NUM
      = 256;
  public static final String JSON_BUFFER_LIMIT
      = "hds.json.buffer.limit";
  public static final int DEFAULT_JSON_BUFFER_LIMIT
      = 10240;
  public static final String BATCH_OPERATION_LIMIT
      = "hds.operation.batch.limit";
  public static final int DEFAULT_BATCH_OPERATION_LIMIT
      = 30;
  public static final String DEFAULT_BACKUP_FILEEXT = ".bak";
  public static final int DEFAULT_BUFFER_SIZE = 4096;
  // this is the default cell size that maintained by hbase.
  // Fixed part needed by KeyValue format = Key Length + Value Length +
  // Row Length + CF Length + Timestamp + Key Value =
  // ( 4 + 4 + 2 + 1 + 8 + 1) = 20 Bytes
  public static final int DEFAULT_KV_SIZE = 20;

  public static final String DEFAULT_ROWKEY_SEPARATOR = ":";
  public static final String HBASE_NAMESPACE_STRING = "hds.namespace";
  public static final String DEFAULT_HBASE_NAMESAPCE_STRING = "HDS";
  public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("f");
  public static final byte[] DEFAULT_NAME_QUALIFIER = Bytes.toBytes("n");
  public static final byte[] DEFAULT_LOCATION_QUALIFIER = Bytes.toBytes("l");
  public static final byte[] DEFAULT_UPLOAD_TIME_QUALIFIER = Bytes.toBytes("t");
  public static final byte[] DEFAULT_SIZE_QUALIFIER = Bytes.toBytes("s");
  public static final byte[] DEFAULT_VALUE_QUALIFIER = Bytes.toBytes("v");
  public static final byte[] DEFAULT_LINK_QUALIFIER = Bytes.toBytes("k");
  public static final List<byte[]> DEFAULT_PARAMETERS = Arrays.asList(
      DEFAULT_NAME_QUALIFIER,
      DEFAULT_LOCATION_QUALIFIER,
      DEFAULT_UPLOAD_TIME_QUALIFIER,
      DEFAULT_SIZE_QUALIFIER,
      DEFAULT_VALUE_QUALIFIER,
      DEFAULT_LINK_QUALIFIER
  );

  // each row in hds table should have name, location, uploadtime, size,
  // value/link. value and link should not both appear in one row.
  public static final int DEFAULT_ROW_QUALIFIER_NUMBER
      = DEFAULT_PARAMETERS.size() - 1;

  public static final String MAPPING_TABLE_NAME = "MAPPING";
  public static final byte[] MAPPING_HOST_QUALIFIER = Bytes.toBytes("h");
  public static final byte[] MAPPING_USER_QUALIFIER = Bytes.toBytes("u");
  public static final byte[] MAPPING_PASSWORD_QUALIFIER = Bytes.toBytes("p");
  public static final byte[] MAPPING_PORT_QUALIFIER = Bytes.toBytes("o");
  public static final byte[] MAPPING_DOMAIN_QUALIFIER = Bytes.toBytes("d");
  public static final List<byte[]> MAPPING_QUALIFIERS = Arrays.asList(
      MAPPING_HOST_QUALIFIER,
      MAPPING_USER_QUALIFIER,
      MAPPING_PASSWORD_QUALIFIER,
      MAPPING_PORT_QUALIFIER,
      MAPPING_DOMAIN_QUALIFIER
  );
  public static final TableName MAPPING_TABLE = TableName.valueOf(
      DEFAULT_HBASE_NAMESAPCE_STRING,
      MAPPING_TABLE_NAME);

  public static String FTP_CONNECTION_TIMEOUT = "hds.ftp.connection.timeout";
  public static String FTP_POOL_CHECKTIME = "hds.ftp.connection.check.period";
  public static String FTP_POOL_SIZE = "hds.ftp.connection.limit";
  public static String FTP_TIME_FORMAT = "yyyyMMddHHmmss.SSS";

  public static long DEFAULT_FTP_CONNECTION_TIMEOUT
      = TimeUnit.MILLISECONDS.convert(10L, TimeUnit.SECONDS);
  public static int DEFAULT_FTP_POOL_CHECKTIME = 3;
  public static int DEFAULT_FTP_POOL_SIZE = 50;

  public static final String HTTP_SERVER_LONG_TERM_TASK_NUM = "hds.httpserver.task.long.num";
  public static final int DEFAULT_HTTP_SERVER_LONG_TERM_TASK_NUM = 150;
  public static final String HTTP_SERVER_SHORT_TERM_TASK_NUM = "hds.httpserver.task.short.num";
  public static final int DEFAULT_HTTP_SERVER_SHORT_TERM_TASK_NUM = 100;

  public static final String HDS_AUTH_FILE_LOCATION = "hds.auth.location";
  public static final String HDS_AUTH_ENABLE = "hds.auth.enable";
  public static final boolean DEFAULT_HDS_AUTH_ENABLE = false;
  public static final String HDS_AUTH_UPDATE_PERIOD = "hds.auth.update.period";
  public static final long DEFAULT_HDS_AUTH_UPDATE_PERIOD
      = TimeUnit.SECONDS.toSeconds(5);

  public static final String DATASERVICE_API_VERSION = "v1";
  public static final String HDS_VERSION = "v1";
  public static final String HDS_VERSION_V2 = "v2";
  public static final String API_ACCESS = "/access";
  public static final String API_DELETE = "/delete";
  public static final String API_BATCHDELETE = "/batchdelete";
  public static final String API_LIST = "/list";
  public static final String API_LIST2 = "/list2";
  public static final String API_ADDMAPPING = "/addmapping";
  public static final String API_DELETEMAPPING = "/deletemapping";
  public static final String API_LISTMAPPING = "/listmapping";
  public static final String API_WATCH = "/watch";
  public static final String API_RUN = "/run";
  public static final String API_LOADING = "/loading";
  public static final String API_KILL = "/kill";
  public static final String API_KILL_DRS = "/killdrs";
  public static final String API_WATCH_DRS = "/watchdrs";

  public static final String HTTP_DEFAULT_ACCESS_URL = "/dataservice/"
      + HDS_VERSION + API_ACCESS;
  public static final String HTTP_DEFAULT_DELETE_URL = "/dataservice/"
      + HDS_VERSION + API_DELETE;
  public static final String HTTP_DEFAULT_BATCH_DELETE_URL = "/dataservice/"
      + HDS_VERSION + API_BATCHDELETE;
  public static final String HTTP_DEFAULT_LIST_URL = "/dataservice/"
      + HDS_VERSION + API_LIST;
  public static final String HTTP_DEFAULT_LIST_URL2 = "/dataservice/"
          + HDS_VERSION + API_LIST2;
  public static final String HTTP_DEFAULT_MAPPING_ADD_URL = "/dataservice/"
      + HDS_VERSION + API_ADDMAPPING;
  public static final String HTTP_DEFAULT_MAPPING_DELETE_URL = "/dataservice/"
      + HDS_VERSION + API_DELETEMAPPING;
  public static final String HTTP_DEFAULT_MAPPING_LIST_URL = "/dataservice/"
      + HDS_VERSION + API_LISTMAPPING;
  public static final String HTTP_DEFAULT_WATCH_URL = "/dataservice/"
      + HDS_VERSION + API_WATCH;
  public static final String HTTP_DEFAULT_RUN_URL = "/dataservice/"
      + HDS_VERSION + API_RUN;
  public static final String HTTP_DEFAULT_LOADING_URL = "/dataservice/"
      + HDS_VERSION + API_LOADING;
  public static final String HTTP_DEFAULT_KILL_URL = "/dataservice/"
      + HDS_VERSION + API_KILL;
  public static final String HTTP_DEFAULT_KILL_DRS_URL = "/dataservice/"
      + HDS_VERSION + API_KILL_DRS;
  public static final String HTTP_DEFAULT_WATCH_DRS_URL = "/dataservice/"
      + HDS_VERSION + API_WATCH_DRS;
  public static final String HTTP_V2_ACCESS_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_ACCESS;
  public static final String HTTP_V2_LIST_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_LIST;
  public static final String HTTP_V2_DELETE_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_DELETE;
  public static final String HTTP_V2_BATCH_DELETE_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_BATCHDELETE;
  public static final String HTTP_V2_MAPPING_ADD_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_ADDMAPPING;
  public static final String HTTP_V2_MAPPING_DELETE_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_DELETEMAPPING;
  public static final String HTTP_V2_MAPPING_LIST_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_LISTMAPPING;
  public static final String HTTP_V2_WATCH_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_WATCH;
  public static final String HTTP_V2_LOADING_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_LOADING;
  public static final String HTTP_V2_KILL_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_KILL;
  public static final String HTTP_V2_RUN_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_RUN;
  public static final String HTTP_V2_KILL_DRS_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_KILL_DRS;
  public static final String HTTP_V2_WATCH_DRS_URL = "/dataservice/"
      + HDS_VERSION_V2 + API_WATCH_DRS;
  public static enum API {
    API_ACCESS,
    API_DELETE,
    API_BATCHDELETE,
    API_LIST,
    API_ADDMAPPING,
    API_DELETEMAPPING,
    API_LISTMAPPING,
    API_WATCH,
    API_RUN,
    API_LOADING,
    API_KILL,
    API_DRS_KILL
  }

  public static final String DRS_APPLICATIONID = "applicationid";

  public static final int HTTP_OK_STATUS = 200;
  public static final int HTTP_NOT_FOUND_STATUS = 404;
  public static final String HTTP_NOT_FOUND_MESSAGE = "404 not fuond";
  public static final int HTTP_FORBIDDEN = 403;
  public static final String HTTP_BAD_REQUEST_MESSAGE = "Bad Request";
  public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
  public static final int HTTP_REDIRECT_ERROR_GET = 302;
  public static final int HTTP_REDIRECT_ERROR_POST = 307;
  public static final String HTTP_INTERNAL_SERVER_ERROR_MESSAGE = "Internal server error";
  public static final String HTTP_SERVER_ADDRESS = "hds.httpserver.address";
  public static final String HTTP_SERVER_PORT = "hds.httpserver.port";
  public static final int DEFAULT_HTTP_SERVER_PORT = 8000;

  public static final int DEFAULT_LIST_LIMIT = 100;
  public static final String LIST_DATAINFO = "dataInfo";
  public static final String LIST_URI = "uri";
  public static final String LIST_NAME = "name";
  public static final String LIST_SIZE = "size";
  public static final String LIST_TIME = "ts";
  public static final String LIST_DATAOWNER = "dataowner";
  public static final String LIST_FILETYPE = "type";
  public static final String LIST_DATAOWNER_HOST = "host";
  public static final String LIST_DATAOWNER_RATIO = "ratio";
  public static final String LIST_ERROR_MESSAGE = "errormessage";

  public static final String KILL_DRS_MESSAGE = "message";

  public static final String[] BATCH_DELETE_LEGAL_ARGS = {
    DS_URI_ARG_FILE, DS_URI_ARG_DIRECTORY, HDS_URI_ARG_KEY, HDS_URI_ARG_MINKEY,
    HDS_URI_ARG_MAXKEY, HDS_URI_ARG_TIME, HDS_URI_ARG_MINTIME,
    HDS_URI_ARG_MAXTIME, HDS_URI_ARG_SIZE, HDS_URI_ARG_MINSIZE,
    HDS_URI_ARG_MAXSIZE, HDS_URI_ARG_NAME, HDS_URI_ARG_LOCATION
  };
  public static final String[] BATCH_DELETE_LEGAL_ARGS_V2 = {
    HDS_URI_ARG_TIME, HDS_URI_ARG_MINTIME,
    HDS_URI_ARG_MAXTIME, HDS_URI_ARG_SIZE, HDS_URI_ARG_MINSIZE,
    HDS_URI_ARG_MAXSIZE, HDS_URI_ARG_NAME, HDS_URI_ARG_LOCATION
  };
  public static final String[] DELETE_MAPPING_LEGAL_ARGS = {
    MAPPING_ARG_ID, MAPPING_ARG_HOST,
    MAPPING_ARG_PORT, MAPPING_ARG_MINPORT, MAPPING_ARG_MAXPORT,
    MAPPING_ARG_USER, MAPPING_ARG_DOMAIN, MAPPING_ARG_PASSWD
  };

  public static final String DEFAULT_HTTP_POST_FILE_NAME = "tmpFile";

  public static final String LIST_LOCATION = "location";
  public static final String ERROR_RESPONSE = "RemoteException";
  public static final String ERROR_RESPONSE_JAVACLASSNAME = "javaClassName";
  public static final String ERROR_RESPONSE_EXCEPTION = "exception";
  public static final String ERROR_RESPONSE_MESSAGE = "message";

  public static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  public static final String DEFAULT_TIME_ZONE = "Asia/Taipei";

  public static final String DEFAULT_CHAR_ENCODING = "UTF-8";
  public static final String DEFAULT_DOWNLOAD_FILE_NAME = "download";

  public static final String GLOBAL_LOCK_DISABLE = "hds.lock.disable";

  public static final String HODING_READ_LOCK = " source cannot be read since someone is holding lock";
  public static final String HODING_WRITE_LOCK = " source cannot be written since someone is holding lock";

  public static final String HDS_METRIC_HISTORY_TTL = "hds.metric.history.ttl";
  public static final long DEFAULT_METRIC_HISTORY_TTL = 60l;

  public static final String HDS_METRIC_UPDATE_PERIOD = "hds.metric.update.period";
  public static final long DEFAULT_HDS_METRIC_UPDATE_PERIOD = 5l;

  public static final String TASK_HEARTBEAT = "hds.task.heartbeat";
  public static final long DEFAULT_TASK_HEARTBEAT
      = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);

  public static final String BALANCER_COMAPARE_MAX_NUM = "hds.balancer.maxcompareserver";
  public static final int DEFAULT_BALANCER_COMAPARE_MAX_NUM = 2;

  public static final String READ_BYTES_COST_COEFFICIENT = "hds.balancer.cost.coefficient.read.bytes";
  public static final double DEFAULT_READ_BYTES_COST_COEFFICIENT = 1;

  public static final String READ_REQUEST_COST_COEFFICIENT = "hds.balancer.cost.coefficient.read.request";
  public static final double DEFAULT_READ_REQUEST_COST_COEFFICIENT = 1;

  public static final String WRITE_BYTES_COST_COEFFICIENT = "hds.balancer.cost.coefficient.write.bytes";
  public static final double DEFAULT_WRITE_BYTES_COST_COEFFICIENT = 1;

  public static final String WRITE_REQUEST_COST_COEFFICIENT = "hds.balancer.cost.coefficient.write.request";
  public static final double DEFAULT_WRITE_REQUEST_COST_COEFFICIENT = 1;

  public static final String PAST_PROPORTION = "hds.balancer.cost.coefficient.cost.past.proportion";
  public static final double DEFAULT_PAST_PROPORTION = 0.5;

  public static final String SERVER_WEIGHT_COEFFICIENT = "hds.balancer.cost.server.weight";
  public static final double DEFAULT_SERVER_WEIGHT_COEFFICIENT = 1;

  public static final String READ = "read";
  public static final String WRITE = "write";

  public static enum LOADINGCATEGORY {
    READ,
    WRITE
  }

  public static final String JDBC_CSV_FORMAT_QUERY = "format";
  public static final String JDBC_DEFAULT_CSV_FORMAT = "Default";
  public static final String JDBC_WITH_HEADER_QUERY = "header";
  public static final boolean JDBC_DEFAULT_WITH_HEADER = false;

  public static final String JDBC_INFO_QUERY = "info";
  public static final String JDBC_QUERY_QUERY = "query";
  public static final String JDBC_TABLE_QUERY = "table";
  public static String JDBC_QUERY_FILE = "file";

  public static String JDBC_CONNECTION_TIMEOUT = "hds.jdbc.connection.timeout";
  public static String JDBC_POOL_CHECKTIME = "hds.jdbc.connection.check.period";
  public static String JDBC_POOL_SIZE = "hds.jdbc.connection.limit";

  public static long DEFAULT_JDBC_CONNECTION_TIMEOUT
      = TimeUnit.MILLISECONDS.convert(10L, TimeUnit.SECONDS);
  public static int DEFAULT_JDBC_POOL_CHECKTIME = 3;
  public static int DEFAULT_JDBC_POOL_SIZE = 50;

  public static String CSV_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";



  public static String HBASE_MAX_CELL_SIZE = "hbase.client.keyvalue.maxsize";
  public static int DEFAULT_HBASE_MAX_CELL_SIZE = 10485760;

  public static String JDBC_CSV_BUFFER_QUEUE_SIZE = "hds.jdbc.buffer.queue.size";
  public static int DEFAULT_JDBC_CSV_BUFFER_QUEUE_SIZE = 5;

  public static String JDBC_CSV_BUFFER_SIZE = "hds.jdbc.buffer.size";
  public static int DEFAULT_JDBC_CSV_BUFFER_SIZE = 1024;

  public static String LOCK_NODE_TIME_TO_CLEAN = "hds.locknode.time_to_clean";
  public static long DEFAULT_LOCK_NODE_TIME_TO_CLEAN = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  public static final String LOGGER_ENABLE = "hds.logger.enable";
  public static final boolean DEFAULT_LOGGER_ENABLE = true;
  public static final String LOG_DB_TABLE_NAME = "HDSLOG";

  public static final String CREATE_LOCK_MAX_RETRTY = "hds.lock.retry";
  public static final int DEFAULT_CREATE_LOCK_MAX_RETRTY = 3;

  /* >>> 2021/10/29 Brad added for CORS support >>> */
  public static final String CORS_ALLOW = "hds.httpserver.cors.allow";
  public static final boolean DEFAULT_CORS_ALLOW = false;

  public static final String CORS_ORIGINS = "hds.httpserver.cors.origins";
  public static final String DEFAULT_CORS_ORIGINS = "*";
  /* <<< 2021/10/29 Brad added for CORS support <<< */
}
