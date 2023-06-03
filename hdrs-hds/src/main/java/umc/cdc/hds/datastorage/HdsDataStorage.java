package umc.cdc.hds.datastorage;

import umc.cdc.hds.datastorage.status.DataRecord;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import umc.cdc.hds.core.Protocol;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.MAX_KEYVALUE_SIZE_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import umc.cdc.hds.core.HBaseConnection;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.HDSUtil;
import umc.cdc.hds.core.SingletonConnectionFactory;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecordBuilder;
import umc.cdc.hds.datastorage.status.ErrorInfo;
import umc.cdc.hds.exceptions.HdsDataStorageException;
import umc.cdc.hds.exceptions.IllegalUriException;
import umc.cdc.hds.tools.CloseableCacheIterable;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.uri.DataStorageQuery;
import umc.cdc.hds.uri.UriRequest;
import umc.cdc.hds.tools.SharedMemory;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriParser;

/**
 *
 * @author brandboat
 */
public class HdsDataStorage implements DataStorage {

  private static final Log LOG = LogFactory.getLog(HdsDataStorage.class);
  private static final Set<TableName> cachedTableSet = new HashSet();
  private final HBaseConnection conn;
  private final FileSystem fs;
  private final SharedMemory sharedMem;
  private final Path rootPath;
  private int threshold;
  private final int dirNum;
  private final String namespace;
  private final int batchDeleteNum;

  private static HTableDescriptor getHTableDescriptor(final TableName tableName) {
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor colDesc = new HColumnDescriptor(HDSConstants.DEFAULT_FAMILY);
    colDesc.setCompressionType(Compression.Algorithm.GZ);
    colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    tableDesc.addFamily(colDesc);
    return tableDesc;
  }

  private static Get toGet(final String key) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    get.addFamily(HDSConstants.DEFAULT_FAMILY);
    return get;
  }

  private static void deleteQualifier(Table t, byte[] key, byte[] qualifier)
          throws IOException {
    Delete deleteQual = new Delete(key);
    deleteQual.addColumns(HDSConstants.DEFAULT_FAMILY, qualifier);
    t.delete(deleteQual);
  }

  private static void deleteFile(FileSystem fs, Path filePath)
          throws IOException {
    boolean isDeleted = fs.delete(filePath, false);
    if (!isDeleted) {
      throw new IOException("File delete failed.");
    }
  }

  private static void deleteFileIfExist(FileSystem fs, Path filePath)
          throws IOException {
    boolean isExist = fs.exists(filePath);
    if (isExist) {
      deleteFile(fs, filePath);
    }
  }

  public HdsDataStorage(final Configuration conf) throws IOException {
    this.conn = SingletonConnectionFactory.createConnection(conf);
    this.fs = FileSystem.get(conf);
    this.namespace = conf.get(
            HDSConstants.HBASE_NAMESPACE_STRING,
            HDSConstants.DEFAULT_HBASE_NAMESAPCE_STRING);
    try (Admin admin = conn.getAdmin()) {
      HDSUtil.createNamespaceIfNotExisted(conn, namespace);
      TableName[] hdsTables = admin.listTableNamesByNamespace(namespace);
      synchronized (cachedTableSet) {
        cachedTableSet.addAll(Arrays.asList(hdsTables));
      }
    }
    int hbaseKvSize = conf.getInt(MAX_KEYVALUE_SIZE_KEY,
            MAX_KEYVALUE_SIZE_DEFAULT);
    threshold = -1;
    if (hbaseKvSize <= 0) {
      threshold = conf.getInt(
              HDSConstants.LARGE_DATA_THRESHOLD,
              HDSConstants.DEFAULT_LARGE_DATA_THRESHOLD);
    } else {
      threshold = hbaseKvSize;
    }
    final int totalMemory = conf.getInt(
            HDSConstants.MEMORY_BUFFER_UPPER_LIMIT,
            HDSConstants.DEFAULT_MEMORY_BUFFER_UPPER_LIMIT);
    final int queueSize = (totalMemory < threshold)
            ? 1
            : (totalMemory / threshold);
    final long timeout = conf.getLong(
            HDSConstants.MEMORY_ALLOCATE_TIMEOUT,
            HDSConstants.DEFAULT_MEMORY_ALLOCATE_TIMEOUT);
    LOG.info("threshold : " + threshold + " bytes, queue size : "
            + queueSize);
    this.sharedMem = new SharedMemory(queueSize, threshold, timeout);
    this.rootPath = new Path(conf.get(
            HDSConstants.HDS_PATH,
            HDSConstants.DEFAULT_HDS_PATH));
    this.dirNum = conf.getInt(
            HDSConstants.HDS_DIR_NUM,
            HDSConstants.DEFAULT_HDS_DIR_NUM);
    this.batchDeleteNum = conf.getInt(
            HDSConstants.BATCH_OPERATION_LIMIT,
            HDSConstants.DEFAULT_BATCH_OPERATION_LIMIT);
  }

  public boolean renameHdfsFile(Path src, Path dst) throws IOException {
    final long ts = System.currentTimeMillis();
    String sFrom = "hds://" + src.toUri().getPath() + "?ver=v2";
    String sTo = "hds://" + dst.toUri().getPath() + "?ver=v2";
    HdsUriParser hFrom = new HdsUriParser(UriParser.valueOf(sFrom, null));
    HdsUriParser hTo = new HdsUriParser(UriParser.valueOf(sTo, null));
    String kFrom = hFrom.getKey().get().concat(hFrom.getName());
    String kTo = hTo.getKey().get().concat(hTo.getName());
    Table table = conn.getTable(TableName.valueOf(namespace, hFrom.getCatalog()));
    Record record = new Record(table.get(toGet(kFrom)));
    Path pTo = createHDSLargeFilePath(ts, hTo.getCatalog(), kTo, true);
    byte[] key = Bytes.toBytes(kFrom);
    Put put = new Put(Bytes.toBytes(kTo));
    put.addColumn(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_NAME_QUALIFIER,
            ts,
            Bytes.toBytes(dst.getName()));
    put.addColumn(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER,
            ts,
            Bytes.toBytes(ts));
    put.addColumn(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_SIZE_QUALIFIER,
            ts,
            Bytes.toBytes(record.getSize()));
    put.addColumn(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_LOCATION_QUALIFIER,
            ts,
            Protocol.hdfs.getCode());
    put.addColumn(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_LINK_QUALIFIER,
            ts,
            Bytes.toBytes(pTo.toString()));
    if (fs.rename(new Path(record.getLink()), pTo)) {

      // delete meta
      table.delete(new Delete(key));
      table.close();

      // insert meta
      try (Table t = conn.getTable(TableName.valueOf(namespace, hTo.getCatalog()))) {
        t.put(put);
      }
      delNnusedDir(new Path(record.getLink()).getParent());
    } else {
      return false;
    }

    return true;
  }
  
  private void delNnusedDir(Path path) {
    Path p = new Path(path.toUri().getPath());
    try {
      if (p.depth() <= 1 || !fs.delete(p, false)) {
      } else {
        delNnusedDir(p.getParent());
      }
    } catch (IOException ex) {
    }
  }

  @Override
  public DataStorageOutput create(UriRequest req) throws IOException {
    return new HdsDataOutput(new HdsUriParser(req));
  }

  @Override
  public DataStorageInput open(UriRequest req) throws IOException {
    return new HdsDataInput(new HdsUriParser(req));
  }

  @Override
  public void close() throws IOException {
    HDSUtil.closeWithLog(conn, LOG);
    HDSUtil.closeWithLog(sharedMem, LOG);
  }

  @Override
  public DataRecord delete(UriRequest req, boolean isRecursive) throws IOException {
    return delete(new HdsUriParser(req), isRecursive);
  }

  @Override
  public CloseableIterator<BatchDeleteRecord> batchDelete(
          UriRequest req,
          boolean enableWildcard) throws IOException {
    return batchDelete(new HdsUriParser(req), enableWildcard);
  }

  @Override
  public CloseableIterator<DataRecord> list(UriRequest req, Query optArg) throws IOException {
    return list(new HdsUriParser(req), optArg);
  }

  public DataRecord delete(HdsUriParser hdsParser, boolean isRecursive) throws IOException {
    final String key;
    // final boolean isHdsV2 = hdsParser.getVersion().equals(HDSConstants.HDS_VERSION_V2);
    final boolean isHdsV2 = true;
    final boolean isRoot = hdsParser.uri.split("/+").length == 2;
    final boolean isDirectory = hdsParser.uri.endsWith("/");
    if (isHdsV2) {
      if (hdsParser.getName().equals("")) {
        if (isRoot) {
          key = "*" + HDSConstants.DEFAULT_ROWKEY_SEPARATOR + "*";
        } else {
          key = hdsParser.getPrefixOfKey(true, true);
        }
      } else {
        key = hdsParser.getKey().orElseThrow(() -> {
          return new IOException("Hds Key not specified.");
        }) + hdsParser.getName();
      }
    } else {
      key = hdsParser.getKey().orElseThrow(() -> {
        return new IOException("Hds Key not specified.");
      });
    }
    final String catalog = hdsParser.getCatalog();
    final TableName tableName = TableName.valueOf(
            HDSConstants.DEFAULT_HBASE_NAMESAPCE_STRING, catalog);
    try (Table t = conn.getTable(tableName)) {
      DataRecord le;
      Result r = null;
      if (!isRoot) {
        r = t.get(toGet(key));
        if (r.isEmpty()) {
          throw new IOException(
                  HDSUtil.buildHdsDataUri(
                          hdsParser.getCatalog(), key, hdsParser.getVersion())
                  + " not found.");
        }
        le = resultToListElement(r, tableName, hdsParser.getVersion());
      } else {
        le = new DataRecord(hdsParser.getUri(), Protocol.hbase, hdsParser.catalog, 0, 0, DataRecord.FileType.directory, new TreeMap<>());
      }
      if (isHdsV2 && isDirectory) {
        Optional<String> dirKey;
        if (isRoot) {
          dirKey = Optional.of(key);
        } else {
          dirKey = Optional.of(hdsParser.getPrefixOfKey(false) + "*");
        }
        hdsParser.query.put(HDSConstants.DS_URI_ARG_DIRECTORY, "true");
        final Scan scan = createScan(hdsParser, dirKey, true);
        final ResultScanner rs = t.getScanner(scan);
        List<Result> needDelete = new ArrayList<>();
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
          r = it.next();
          needDelete.add(r);
        }
        if ((isRoot && needDelete.size() > 0 && isRecursive == false)
                || (isRecursive == false && needDelete.size() > 1)) {
          throw new IOException(
                  HDSUtil.buildHdsDataUri(
                          hdsParser.getCatalog(), hdsParser.getKey().orElse(""), hdsParser.getVersion())
                  + ": Directory not empty");
        }
        batchDelete(t, needDelete, hdsParser.getVersion());
        rs.close();
      } else {
        delete(t, r);
      }
      return le;
    }
  }

  private CloseableIterator<BatchDeleteRecord> batchDelete(HdsUriParser hdsParser,
          boolean enableWildcard) throws IOException {
    final String catalog = hdsParser.getCatalog();
    final TableName tn = TableName.valueOf(namespace, catalog);
    final Table t = conn.getTable(tn);
    final Optional<String> key;
    if (hdsParser.getVersion().equals(HDSConstants.HDS_VERSION_V2)) {
      hdsParser.getQuery().put(HDSConstants.DS_URI_ARG_DIRECTORY, "false");
      hdsParser.getQuery().put(HDSConstants.DS_URI_ARG_FILE, "true");
      key = Optional.of(hdsParser.getPrefixOfKey(true)
              + hdsParser.getQuery().getName().orElse("*"));
      if (!hdsParser.getQuery().getName().isPresent()) {
        enableWildcard = true;
      }
    } else {
      key = hdsParser.getQuery().getKey();
    }
    final Scan scan = createScan(hdsParser, key, enableWildcard);
    return new CloseableCacheIterable<BatchDeleteRecord>() {
      private boolean resultIsEmpty = false;
      private final ResultScanner rs = t.getScanner(scan);

      @Override
      public Queue<BatchDeleteRecord> setCache() throws IOException {
        Queue needDelete = new LinkedList<>(
                getNeedDelete().orElse(new LinkedList<>()));
        return needDelete;
      }

      @Override
      public void close() {
        rs.close();
        HDSUtil.closeWithLog(t, LOG);
      }

      private Optional<List<BatchDeleteRecord>> getNeedDelete()
              throws IOException {
        List<Result> needDelete = new ArrayList<>();
        for (int i = 0; i < batchDeleteNum; i++) {
          Result r = rs.next();
          if (resultIsEmpty) {
            return Optional.empty();
          }
          if (r == null) {
            resultIsEmpty = true;
            break;
          }
          needDelete.add(r);
        }
        List<BatchDeleteRecord> fails = batchDelete(t, needDelete, hdsParser.getVersion());
        if (fails.isEmpty()) {
          getNeedDelete();
        }
        return Optional.of(fails);
      }
    }.iterator();
  }

  private List<BatchDeleteRecord> batchDelete(Table t, List<Result> resultList, String version)
          throws IOException {
    List<Delete> deletes = new ArrayList<>();
    List<DataRecordBuilder> le = new LinkedList<>();
    for (Result r : resultList) {
      Optional<byte[]> key = Optional.ofNullable(r.getRow());
      Protocol location = Protocol.find(r.getValue(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_LOCATION_QUALIFIER)).orElse(null);
      deletes.add(new Delete(key.orElseThrow(() -> {
        return new IOException("failed to find key");
      })));
      le.add(resultToListElementBuilder(r, t.getName(), version));
      try {
        if (location.equals(Protocol.hdfs)) {
          Optional<byte[]> link = Optional.ofNullable(r.getValue(
                  HDSConstants.DEFAULT_FAMILY,
                  HDSConstants.DEFAULT_LINK_QUALIFIER));
          deleteFile(fs, new Path(Bytes.toString(
                  link.orElseThrow(() -> {
                    return new IOException(new String(r.getRow())
                            + " Failed to find link.");
                  }))));
          delNnusedDir(new Path(Bytes.toString(link.get())).getParent());
        }
      } catch (IOException ex) {
        LOG.error(ex);
      }
    }
    try {
      t.delete(deletes);
    } catch (IOException ex) {
      // if delete list is not empty, it means there are fails deletes,
      // return fail deletes.
      for (int i = 0; i < le.size(); i++) {
        for (Delete d : deletes) {
          String hdsUri = HDSUtil.buildHdsDataUri(
                  t.getName().getNameAsString(),
                  Bytes.toString(d.getRow()), version);
          if (le.get(i).build().getUri().equals(hdsUri)) {
            le.remove(i);
          }
        }
      }
      List<BatchDeleteRecord> fails = new LinkedList<>();

      le.forEach(l -> {
        fails.add(new BatchDeleteRecord(l.build(), new ErrorInfo(ex)));
      });
      return fails;
    }
    return new LinkedList<>();
  }

  private void delete(Table t, Result r)
          throws HdsDataStorageException, IOException {
    Optional<byte[]> link = Optional.ofNullable(r.getValue(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_LINK_QUALIFIER)),
            key = Optional.ofNullable(r.getRow());
    Protocol location = Protocol.find(r.getValue(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_LOCATION_QUALIFIER)).orElse(null);

    switch (location) {
      case hdfs:
        t.delete(new Delete(key.orElseThrow(() -> {
          return new HdsDataStorageException(
                  "Failed to find row key.");
        })));
        fs.delete(new Path(Bytes.toString(link.orElseThrow(() -> {
          return new HdsDataStorageException("Failed to find link.");
        }))), false);
        delNnusedDir(new Path(Bytes.toString(link.get())).getParent());
        break;
      case hbase:
        t.delete(new Delete(key.orElseThrow(() -> {
          return new HdsDataStorageException(
                  "Failed to find row key.");
        })));
        break;
      default:
        throw new HdsDataStorageException("Unsupported location: "
                + location);
    }
  }

  private boolean findSpec(Optional<String> key) {
    return key.isPresent() ? !HDSUtil.isWildCard(key.get()) : false;
  }

  /**
   * list API - search data in hds datastorage, use scan when search range of
   * data, otherwise use get.
   *
   * @param hdsParser
   * @param optArg
   * @return
   * @throws HdsDataStorageException
   * @throws IllegalUriException
   * @throws IOException
   */
  private CloseableIterator<DataRecord> list(HdsUriParser hdsParser, Query optArg)
          throws HdsDataStorageException, IllegalUriException, IOException {
    final String catalog = hdsParser.getCatalog();
    final TableName tn = TableName.valueOf(namespace, catalog);
    final Optional<String> key;
    final Table t = conn.getTable(tn);
    final int offset = optArg.getOffset();
    final int limit = optArg.getLimit();
    if (hdsParser.getVersion().equals(HDSConstants.HDS_VERSION_V2)) {
      key = Optional.of(hdsParser.getPrefixOfKey(true)
              + hdsParser.getQuery().getKey().orElse("*"));
    } else {
      key = hdsParser.getQuery().getKey();
    }
    if (findSpec(key)) {
      final Get get = createGet(key);
      return new CloseableIterator<DataRecord>() {
        private Result next = t.get(get);

        {
          if (next.isEmpty()) {
            next = null;
          }
        }

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public DataRecord next() {
          if (next == null) {
            return null;
          }
          DataRecord dr = null;
          if (1 > offset && limit > 0) {
            dr = resultToListElement(next, tn, hdsParser.getVersion());
          }
          next = null;
          return dr;
        }

        @Override
        public void close() {
          HDSUtil.closeWithLog(t, LOG);
        }
      };
    } else {
      final Scan scan = createScan(hdsParser, key, true);
      return new CloseableIterator<DataRecord>() {
        private final ResultScanner rs = t.getScanner(scan);
        private int position = 0;
        private Result next = rs.next();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public DataRecord next() {
          if (next == null) {
            return null;
          }

          DataRecord le;
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
            le = resultToListElement(next, tn, hdsParser.getVersion());
            next = rs.next();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return le;
        }

        @Override
        public void close() {
          rs.close();
          HDSUtil.closeWithLog(t, LOG);
        }
      };
    }
  }

  public long getSharedMemoryTotal() {
    return sharedMem == null ? 0 : sharedMem.getQueueSize();
  }

  public long getSharedMemoryTaken() {
    return sharedMem == null ? 0 : sharedMem.getTakenNum();
  }

  /**
   * Create Get for specific data.
   */
  private Get createGet(Optional<String> rowKey) throws IOException {
    if (!rowKey.isPresent()) {
      throw new IOException("row key not found.");
    }
    return new Get(Bytes.toBytes(rowKey.get()));
  }

  /**
   * Create Scan for list API.
   *
   * @param hdsUri
   * @param enableWildcard
   * @return
   */
  private Scan createScan(HdsUriParser hdsUri, Optional<String> key, boolean enableWildcard) {
    final DataStorageQuery query = hdsUri.getQuery();
    List<Filter> filters = new ArrayList<>();
    filters.add(new SingleColumnValueExcludeFilter(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_VALUE_QUALIFIER,
            CompareFilter.CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes("")));
    if (!query.getQueryValueAsBoolean(HDSConstants.DS_URI_ARG_FILE, true)) {
      filters.add(new RowFilter(
              CompareFilter.CompareOp.EQUAL,
              getComparable("*/", true)));
    }
    if (!query.getQueryValueAsBoolean(HDSConstants.DS_URI_ARG_DIRECTORY, false)) {
      filters.add(new RowFilter(
              CompareFilter.CompareOp.NOT_EQUAL,
              getComparable("*/", true)));
    }
    if (hdsUri.getVersion().equals(HDSConstants.HDS_VERSION)) {
      filters.add(new RowFilter(
              CompareFilter.CompareOp.NOT_EQUAL,
              getComparable("*:*", true)));
    }
    key.ifPresent(k -> {
      filters.add(new RowFilter(
              CompareFilter.CompareOp.EQUAL,
              getComparable(k, enableWildcard)));
    });
    query.getSize().ifPresent(size -> {
      filters.add(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_SIZE_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(size)));
    });
    query.getName().ifPresent(name -> {
      filters.add(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_NAME_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              getComparable(name, enableWildcard)));
    });
    query.getLocation().ifPresent(location -> {
      if (location != Protocol.hbase && location != Protocol.hdfs) {
        return;
      }
      filters.add(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_LOCATION_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              location.getCode()));
    });
    query.getTime().ifPresent(time -> {
      filters.add(new SingleColumnValueFilter(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(time)));
    });
    if (query.getTime().isPresent() == false) {
      query.getTimeRange().ifPresent(timeRange -> {
        filters.add(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER,
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes(timeRange.getMinValue())
        ));
        filters.add(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER,
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                Bytes.toBytes(timeRange.getMaxValue())
        ));
      });
    }
    if (query.getKey().isPresent() == false) {
      query.getKeyRange().ifPresent(keyRange -> {
        filters.add(new RowFilter(
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(keyRange.getMinValue()))));
        filters.add(new RowFilter(
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(keyRange.getMaxValue()))));
      });
    }
    if (query.getSize().isPresent() == false) {
      query.getSizeRange().ifPresent(sizeRange -> {
        filters.add(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_SIZE_QUALIFIER,
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes(sizeRange.getMinValue())));
        filters.add(new SingleColumnValueFilter(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_SIZE_QUALIFIER,
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                Bytes.toBytes(sizeRange.getMaxValue())));
      });
    }
    Scan scan = new Scan();
    if (filters.isEmpty() == false) {
      scan.setFilter(new FilterList(filters));
    }
    return scan;
  }

  /**
   * If enableWildcard is true, return RegexStringComparator, else return
   * BinaryComparator.
   *
   * @param val
   * @param enableWildcard
   * @return
   */
  public static ByteArrayComparable getComparable(
          String val, boolean enableWildcard) {
    if (enableWildcard) {
      return new RegexStringComparator(HDSUtil.convertWildCardToRegEx(val));
    }
    return new BinaryComparator(Bytes.toBytes(val));
  }

  public DataRecord resultToListElement(Result r, TableName tableName, String version) {
    return resultToListElementBuilder(r, tableName, version).build();
  }

  public DataRecordBuilder resultToListElementBuilder(
          Result r, TableName tableName, String version) {
    if (r == null) {
      return null;
    }
    DataRecordBuilder builder = new DataRecordBuilder();
    Protocol p = Protocol.find(r.getValue(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_LOCATION_QUALIFIER))
            .get();
    builder.setUri(HDSUtil.buildHdsDataUri(tableName.getQualifierAsString(),
            Bytes.toString(r.getRow()), version));
    builder.setName(Bytes.toString(r.getValue(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_NAME_QUALIFIER)));
    builder.setProtocol(p);
    builder.setSize(Bytes.toLong(r.getValue(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_SIZE_QUALIFIER)));

    builder.setUploadTime(Bytes.toLong(r.getValue(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER)));
    if (new String(r.getRow()).endsWith("/")) {
      builder.setFileType(DataRecord.FileType.directory);
    } else {
      builder.setFileType(DataRecord.FileType.file);
    }
    Map<String, Double> dataOwner;
    try {
      switch (p) {
        case hbase:
          dataOwner = HDSUtil.getHBaseDataLocationPercentage(
                  conn, tableName, r.getRow());
          break;
        case hdfs:
          String filePath = new String(r.getValue(
                  HDSConstants.DEFAULT_FAMILY,
                  HDSConstants.DEFAULT_LINK_QUALIFIER));
          dataOwner = HDSUtil.getHdfsDataLocationPercentage(fs,
                  filePath);
          break;
        default:
          throw new IOException("error location " + p);
      }
    } catch (IOException ex) {
      dataOwner = new TreeMap<>();
    }
    builder.setOwner(dataOwner);
    return builder;

  }

  @Override
  public long getConnectionNum() {
    return 0;
  }

  public void mkdirs(Path p) throws IOException {
    String[] v2Path = p.toUri().getPath().split("/+");
    if (v2Path.length < 2) {
      throw new IOException("Illegal Hds Uri forrmat.");
    }
    checkTableExisted(TableName.valueOf(namespace, v2Path[1]), true);
    if (v2Path.length < 3) {
      return;
    }
    List<byte[]> directoryKeys = new LinkedList<>();
    StringBuilder str = new StringBuilder();

    for (int i = 2; i < v2Path.length; i++) {
      str = str.append(v2Path[i]).append("/");
      directoryKeys.add(Bytes.toBytes(String.valueOf(i - 2)
              + HDSConstants.DEFAULT_ROWKEY_SEPARATOR + str.toString()));
    }
    long time = System.currentTimeMillis();
    List<Put> puts = getDirectoryPuts(directoryKeys, time, time);
    try (Table t = conn.getTable(TableName.valueOf(namespace, v2Path[1]))) {
      t.put(puts);
    }
  }

  /**
   * Check if table (catalog) is exist, if not , create table, and create the
   * corresponding HDFS directory.
   *
   * @param tableName
   * @throws IOException
   */
  public boolean checkTableExisted(TableName tableName, boolean createIfNotExist) throws IOException {
    synchronized (cachedTableSet) {
      if (cachedTableSet.contains(tableName)) {
        return true;
      }
      try (Admin admin = conn.getAdmin()) {
        if (admin.tableExists(tableName)) {
          cachedTableSet.add(tableName);
        } else if (createIfNotExist) {
          try {
            admin.createTable(getHTableDescriptor(tableName));
          } catch (TableExistsException ex) {
          }
          cachedTableSet.add(tableName);
          return true;
        }
        return false;
      }
    }
  }

  private List<Put> getDirectoryPuts(List<byte[]> keys, long ts, long uploadTime) {
    List<Put> puts = new LinkedList<>();
    keys.forEach((v) -> {
      String[] k = new String(v).split("/|" + HDSConstants.DEFAULT_ROWKEY_SEPARATOR);
      puts.add(new Put(v).addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_SIZE_QUALIFIER,
              ts,
              Bytes.toBytes(0L)));
      puts.add(new Put(v).addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_NAME_QUALIFIER,
              ts,
              Bytes.toBytes(k[k.length - 1])));
      puts.add(new Put(v).addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER,
              ts,
              Bytes.toBytes(uploadTime)));
      puts.add(new Put(v).addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_LOCATION_QUALIFIER,
              ts,
              Protocol.hbase.getCode()));
    });
    return puts;
  }

  private void checkDirExists(Path p) throws IOException {
    if (!(p.isRoot() || fs.exists(p))) {
      checkDirExists(p.getParent());
      fs.mkdirs(p);
      fs.setPermission(p, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  /**
   * Create large file path for HDS. Large File will be stored in HDFS and the
   * file path is yyyy-mm/num/key. The reason for nested directory is to avoid
   * directory meta data become too large. And yyyy-mm is for quick data
   * deletion since user delete data by date.
   *
   * @param time
   * @param catalog
   * @param rowKey
   * @return
   * @throws IOException
   */
  public Path createHDSLargeFilePath(final long time, final String catalog,
          final String rowKey, final boolean _isHdsV2) throws IOException {
    DateTime dt = new DateTime(
            time,
            DateTimeZone.forID(HDSConstants.DEFAULT_TIME_ZONE));
    int fileHash = rowKey.hashCode(),
            fileDirMod = fileHash % dirNum;
    if (fileDirMod < 0) {
      fileDirMod += dirNum;
    }
    String ymDir = dt.getYear() + "-" + dt.getMonthOfYear();
    String fileName;
    Path p = new Path(rootPath, new Path(catalog, ymDir));
    boolean isHdsV2 = true;
    if (isHdsV2) {
      p = new Path(p, new Path(Integer.toString(fileDirMod), HDSConstants.HDS_VERSION_V2));
      String[] s = rowKey.split(HDSConstants.DEFAULT_ROWKEY_SEPARATOR)[1].split("/");
      for (int i = 0; i < s.length - 1; i++) {
        p = new Path(p, s[i]);
      }
      fileName = HDSConstants.DEFAULT_ROWKEY_SEPARATOR + s[s.length - 1];
    } else {
      p = new Path(p, Integer.toString(fileDirMod));
      fileName = rowKey;
    }
    fileName = URLEncoder.encode(fileName, HDSConstants.DEFAULT_CHAR_ENCODING);
    checkDirExists(p);
    Path filePath = new Path(p, fileName);
    return filePath;
  }

  public class HdsDataInput extends DataStorageInput {

    private final HdsUriParser hdsUri;
    private InputStream input;
    private final Record record;
    private long pos = 0;
    private final boolean inHbase;

    HdsDataInput(HdsUriParser hdsParser) throws IOException {
      this.hdsUri = hdsParser;
      Table table = conn.getTable(TableName.valueOf(
              namespace, hdsUri.getCatalog()));
      final String key;
      if (hdsParser.getVersion().equals(HDSConstants.HDS_VERSION_V2)) {
        key = hdsUri.getKey().orElseThrow(() -> {
          return new IOException("Hds Key not specified.");
        }) + hdsUri.getName();
        if (key.endsWith("/")) {
          throw new IllegalUriException("Cannot open the directory.");
        }
      } else {
        key = hdsUri.getKey().orElseThrow(() -> {
          return new IOException("Hds Key not specified.");
        });
      }
      this.record = new Record(table.get(toGet(key)));
      switch (record.getLocation()) {
        case hdfs:
          input = fs.open(new Path(record.getLink()));
          inHbase = false;
          break;
        case hbase:
          input = new ByteArrayInputStream(record.getValue());
          inHbase = true;
          break;
        default:
          throw new IOException("Not support protocol: "
                  + record.getLocation());
      }
    }

    public synchronized void seek(long targetPos) throws IOException {
      if (targetPos > getSize()) {
        throw new EOFException("Cannot seek after EOF");
      }
      if (targetPos < 0) {
        throw new IOException("Cannot seek to a negative offset " + pos);
      }
      if (inHbase) {
        if (pos > targetPos) {
          reopen();
        }
        int diff = (int) (targetPos - pos);
        pos += input.skip(diff);
      } else {
        ((FSDataInputStream) input).seek(targetPos);
      }
    }

    public synchronized long getPos() throws IOException {
      if (inHbase) {
        return pos;
      } else {
        return ((FSDataInputStream) input).getPos();
      }
    }

    private synchronized void reopen() throws IOException {
      if (inHbase) {
        input = new ByteArrayInputStream(record.getValue());
        pos = 0;
      }
    }

    @Override
    public String getUri() {
      return hdsUri.getUri();
    }

    /**
     * If keep arg set false and failover do not happen, delete the data in hds,
     * otherwise close the inputstream.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
      try {
        input.close();
      } finally {
        if (!hdsUri.getQuery().getKeep()) {
          delete(hdsUri, false);
        }
      }
    }

    @Override
    public int read() throws IOException {
      int byteRead = input.read();
      if (byteRead >= 0) {
        pos++;
      }
      return byteRead;
    }

    @Override
    public int read(byte[] b) throws IOException {
      int byteRead = input.read(b, 0, b.length);
      if (byteRead >= 0) {
        pos += byteRead;
      }
      return byteRead;
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      int byteRead = input.read(b, offset, length);
      if (byteRead >= 0) {
        pos += byteRead;
      }
      return byteRead;
    }

    @Override
    public long getSize() {
      return record.getSize();
    }

    @Override
    public String getName() {
      return record.getName();
    }

    @Override
    public void recover() {
    }
  }

  public class HdsDataOutput extends DataStorageOutput {

    private final HdsUriParser hdsUri;
    private final String rowKey;
    private final String catalog;
    private final String fileName;
    private final Protocol location;
    private ByteBuffer buf;
    private Path backupFilePath;
    private Path filePath;
    private boolean isLarge;
    private OutputStream output;
    private long size = 0;
    private boolean hasPutToHBase = false;
    private final TableName tableName;
    private final long ts;
    private final Put put;
    private final long uploadTime;
    private final boolean isHdsV2;

    HdsDataOutput(HdsUriParser hdsUri) throws IOException, HdsDataStorageException {
      LOG.info("[HdsDataOutput] Enter");
      long timeStamp = System.currentTimeMillis();
      this.hdsUri = hdsUri;
    //   this.isHdsV2 = hdsUri.getVersion().equals(HDSConstants.HDS_VERSION_V2);
      this.isHdsV2 = true;
      if (isHdsV2) {
        this.rowKey = hdsUri.getKey().orElseThrow(() -> {
          return new IOException("Hds Key not specified.");
        }) + hdsUri.getName();
        if (rowKey.endsWith("/")) {
          throw new IllegalUriException("Cannot create the directory.");
        }
        this.fileName = hdsUri.getQuery().getName().orElse(hdsUri.getName());
        LOG.info("[HdsDataOutput] isHdsV2, fileName = " + fileName);
      } else {
        this.rowKey = hdsUri.getKey().orElseThrow(() -> {
          return new IOException("Hds Key not specified.");
        });
        this.fileName = hdsUri.getQuery().getName().orElse(rowKey);
        LOG.info("[HdsDataOutput] isNotHdsV2, fileName = " + fileName);
      }
      this.catalog = hdsUri.getCatalog();
      // Data will be stored in HBase at default.
      this.location = hdsUri.getQuery().getLocation().orElse(Protocol.hbase);
      this.isLarge = (location == Protocol.hdfs);
      this.buf = isLarge ? null : sharedMem.take();
      this.tableName = TableName.valueOf(namespace, catalog);
      this.ts = timeStamp;
      this.uploadTime = hdsUri.getQuery().getTime().orElse(timeStamp);
      this.put = new Put(Bytes.toBytes(rowKey));
      this.filePath = isLarge
              ? createHDSLargeFilePath(uploadTime, catalog, rowKey, isHdsV2) : null;
      this.backupFilePath = isLarge
              ? new Path(getTmpPath(filePath.toString())) : null;
      this.output = isLarge ? fs.create(backupFilePath) : null;
      checkTableExisted(tableName, true);
    }

    @Override
    public String getUri() {
      return hdsUri.getUri();
    }

    @Override
    public void recover() throws IOException {
      if (isLarge) {
        HDSUtil.closeWithLog(output, LOG);
        HDSUtil.closeWithLog(() -> deleteFileIfExist(fs, backupFilePath), LOG);
      }
      if (hasPutToHBase) {
        try (Table t = conn.getTable(tableName)) {
          t.delete(new Delete(put.getRow()));
        }
      }
    }

    @Override
    public void write(int b) throws IOException {
      if (isLarge) {
        output.write(b);
      } else if (!isLarge && isSizeOverKVLimit(buf, 1, rowKey.length())) {
        try {
          initHdfsOutput();
          output.write(
                  buf.array(),
                  buf.arrayOffset(),
                  buf.position() - buf.arrayOffset());
          output.write(b);
        } finally {
          if (buf != null) {
            sharedMem.restore(buf);
            buf = null;
          }
        }
      } else {
        buf.put((byte) b);
      }
      ++size;
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      if (isLarge) {
        output.write(b, offset, length);
      } else if (!isLarge
              && isSizeOverKVLimit(buf, length, rowKey.length())) {
        try {
          initHdfsOutput();
          output.write(
                  buf.array(),
                  buf.arrayOffset(),
                  buf.position() - buf.arrayOffset());
          output.write(b, offset, length);
        } finally {
          if (buf != null) {
            sharedMem.restore(buf);
            buf = null;
          }
        }
      } else {
        buf.put(b, offset, length);
      }
      size += (length - offset);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    public String getName() {
      return fileName;
    }

    @Override
    public void close() throws IOException {
      try {
        if (isLarge && output != null) {
          output.close();
        }
      } finally {
        if (buf != null) {
          sharedMem.restore(buf);
          buf = null;
        }
      }
    }

    private void initHdfsOutput() throws IOException {
      isLarge = true;
      if (filePath != null) {
        throw new RuntimeException("The real path should be null");
      }
      if (output != null) {
        throw new RuntimeException("The hdfs output should be null");
      }
      filePath = createHDSLargeFilePath(uploadTime, catalog, rowKey, isHdsV2);
      backupFilePath = new Path(getTmpPath(filePath.toString()));
      output = fs.create(backupFilePath);
    }

    private void renameFile() throws IOException {
      if (fs.exists(filePath)) {
        deleteFile(fs, filePath);
      }
      ((FSDataOutputStream) output).close();
      output = null;
      fs.setPermission(backupFilePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      boolean isRenamed = fs.rename(backupFilePath, filePath);
      if (!isRenamed) {
        throw new IOException("rename failed.");
      }
    }

    private void internalCommit() throws IOException {
      put.addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_NAME_QUALIFIER,
              ts,
              Bytes.toBytes(fileName));
      LOG.info("[HdsDataOutput] internalCommit, fileName = " + fileName);
      put.addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_UPLOAD_TIME_QUALIFIER,
              ts,
              Bytes.toBytes(uploadTime));
      put.addColumn(
              HDSConstants.DEFAULT_FAMILY,
              HDSConstants.DEFAULT_SIZE_QUALIFIER,
              ts,
              Bytes.toBytes(size));
      if (isLarge) {
        put.addColumn(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_LOCATION_QUALIFIER,
                ts,
                Protocol.hdfs.getCode());
        put.addColumn(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_LINK_QUALIFIER,
                ts,
                Bytes.toBytes(filePath.toString()));
      } else {
        byte[] b;
        if (size <= Integer.MAX_VALUE) {
          b = new byte[(int) size];
          for (int i = 0; i < (int) size; i++) {
            b[i] = buf.get(i);
          }
        } else {
          throw new IOException("Small data size is over max integer size");
        }

        put.addColumn(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_LOCATION_QUALIFIER,
                ts,
                Protocol.hbase.getCode());
        put.addColumn(
                HDSConstants.DEFAULT_FAMILY,
                HDSConstants.DEFAULT_VALUE_QUALIFIER,
                ts,
                b);
      }
      try (Table t = conn.getTable(tableName)) {
        clearOldData(isLarge, t, rowKey);
        if (isLarge) {
          renameFile();
        }
        List<Put> puts = getDirectoryPuts(hdsUri.getDirectoryKeys(), ts, uploadTime);
        puts.add(put);
        t.put(puts);
      }
      hasPutToHBase = true;
    }

    private void clearOldData(boolean isNewDataLarge, Table t,
            String clearKey) throws IOException {
      Result r = t.get(toGet(clearKey));
      if (r.isEmpty()) {
        return;
      }
      Record record = new Record(r);
      Protocol p = record.getLocation();
      boolean isOldDataLarge = p.equals(Protocol.hdfs);
      // clear hdfs file.
      if (isOldDataLarge) {
        Path file = new Path(record.getLink());
        deleteFile(fs, file);
      }
      byte[] key = r.getRow();
      // clear hbase qualifier.
      if (isOldDataLarge && !isNewDataLarge) {
        deleteQualifier(t, key, HDSConstants.DEFAULT_LINK_QUALIFIER);
      }
      if (!isOldDataLarge && isNewDataLarge) {
        deleteQualifier(t, key, HDSConstants.DEFAULT_VALUE_QUALIFIER);
      }
    }

    @Override
    public void commit() throws IOException {
      doKeepAction(hdsUri.getQuery().getKeep());
    }

    /**
     * If keep is true, keep the data, otherwise clear the data.
     *
     * @param keep uri arg
     * @throws IOException
     */
    public void doKeepAction(boolean keep) throws IOException {
      if (keep) {
        internalCommit();
      } else {
        deleteTmpFile();
      }
    }

    /**
     * delete backup file (tmp file) if it existed.
     *
     * @throws IOException
     */
    public void deleteTmpFile() throws IOException {
      if (isLarge) {
        deleteFile(fs, backupFilePath);
      }
    }

    protected String getTmpPath(String originPath) throws IOException {
      String unUsedTmpPath = originPath + HDSConstants.DEFAULT_BACKUP_FILEEXT;
      boolean use = false;
      boolean first = true;
      while (!use) {
        if (!first) {
          unUsedTmpPath = originPath + HDSConstants.DEFAULT_BACKUP_FILEEXT
                  + "." + UUID.randomUUID().toString();
        } else {
          first = false;
        }
        if (!fs.exists(new Path(unUsedTmpPath))) {
          use = true;
        }
      }
      return unUsedTmpPath;
    }

    /**
     * Check if the incoming byte length added to buffer will let the buffer
     * size over hbase keyvalue upperlimit.
     *
     * @param buf
     * @param incomingByteLen
     * @return boolean
     */
    private boolean isSizeOverKVLimit(
            ByteBuffer buf,
            int incomingByteLen,
            int rowKeySize) {
      int cellTitleSize
              = rowKeySize
              + HDSConstants.DEFAULT_KV_SIZE
              + HDSConstants.DEFAULT_FAMILY.length
              + HDSConstants.DEFAULT_VALUE_QUALIFIER.length;
      int cellContentSize
              = (buf.position() - buf.arrayOffset())
              + incomingByteLen;

      if ((cellTitleSize + cellContentSize) > threshold) {
        return true;
      }
      return false;
    }
  }

  public static class HdsUriParser {

    private final DataStorageQuery query;
    private final Optional<String> key;
    private final String catalog;
    private String uri;
    private final List<byte[]> directoryKeys = new LinkedList<>();
    private final String version;
    private final String name;

    HdsUriParser(final UriRequest req) throws IllegalUriException {
      query = new DataStorageQuery(req.getQuery().getQueries());
    //   version = req.getQuery().getQueryValue(HDSConstants.HDS_URI_ARG_VERSION,
    //           HDSConstants.HDS_VERSION);
      version = "v2";
      uri = req.toRealUri().substring("hds://".length());
      if (uri.contains(HDSConstants.DEFAULT_ROWKEY_SEPARATOR)) {
        throw new IllegalUriException("Illegal character: "
                + HDSConstants.DEFAULT_ROWKEY_SEPARATOR);
      }
      switch (version) {
        case HDSConstants.HDS_VERSION_V2:
          String[] v2Path = uri.split("/+");
          int v2Valid = v2Path.length;
          if (!req.getName().isPresent() && !uri.endsWith("/")) {
            v2Valid--;
            req.setName(v2Path[v2Valid]);
          }
          if (v2Valid == 2) {
            catalog = v2Path[1];
            key = Optional.ofNullable(String.valueOf(v2Valid - 2)
                    + HDSConstants.DEFAULT_ROWKEY_SEPARATOR);
            name = req.getName().orElse("");
          } else if (v2Valid > 2) {
            catalog = v2Path[1];
            StringBuilder str = new StringBuilder();
            for (int i = 2; i < v2Valid; i++) {
              str = str.append(v2Path[i]).append("/");
              directoryKeys.add(Bytes.toBytes(String.valueOf(i - 2)
                      + HDSConstants.DEFAULT_ROWKEY_SEPARATOR + str.toString()));
            }
            str = str.insert(0, String.valueOf(v2Valid - 2)
                    + HDSConstants.DEFAULT_ROWKEY_SEPARATOR);
            name = req.getName().orElse("");
            key = Optional.ofNullable(str.toString());
          } else {
            throw new IllegalUriException("Illegal Hds Uri forrmat.");
          }
          break;

        default:
          String[] path = uri.split("/+");
          int valid = path.length;
          switch (valid) {
            case 3:
              catalog = path[1];
              key = Optional.ofNullable(path[2]);
              name = path[2];
              break;
            case 2:
              catalog = path[1];
              key = Optional.empty();
              name = "";
              break;
            default:
              throw new IllegalUriException("Illegal Hds Uri forrmat.");
          }
      }
      if (catalog == null || catalog.isEmpty()) {
        throw new IllegalUriException("Hds catalog not specified.");
      }
    }

    public String getCatalog() {
      return catalog;
    }

    public Optional<String> getKey() {
      return key;
    }

    public DataStorageQuery getQuery() {
      return query;
    }

    public String getUri() {
      return new StringBuilder(Protocol.hds.name())
              .append("://")
              .append(uri)
              .toString();
    }

    public List<byte[]> getDirectoryKeys() {
      return directoryKeys;
    }

    public String getVersion() {
      return version;
    }

    public String getPrefixOfKey(boolean withValidNumber) {
      return getPrefixOfKey(withValidNumber, false);
    }

    public String getPrefixOfKey(boolean withValidNumber, boolean isParent) {
      String[] path = uri.split("/+");
      StringBuilder str = new StringBuilder();
      if (withValidNumber) {
        if (isParent) {
          str = str.append(path.length - 3);
        } else {
          str = str.append(path.length - 2);
        }
      } else {
        str = str.append("*");
      }
      str = str.append(HDSConstants.DEFAULT_ROWKEY_SEPARATOR);
      for (int i = 2; i < path.length; i++) {
        str = str.append(path[i]).append("/");
      }
      return str.toString();
    }

    public String getName() {
      return this.name;
    }
  }
}

