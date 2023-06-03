package umc.cdc.hds.core;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import umc.cdc.hds.datastorage.DataStorage;
import umc.cdc.hds.datastorage.HdsDataStorage;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.dblog.HdsLog;
import umc.cdc.hds.exceptions.BeLockedException;
import umc.cdc.hds.lock.GlobalLockManager;
import umc.cdc.hds.lock.GlobalLockManagerFactory;
import umc.cdc.hds.lock.Lock;
import umc.cdc.hds.tools.CloseableIterator;
import umc.cdc.hds.tools.StopWatch;
import umc.cdc.hds.uri.Query;
import umc.cdc.hds.uri.UriRequest;

/**
 *
 * @author brandboat
 */
public final class HDS {

  private static Configuration conf;
  private static final CountDownLatch waitLatch = new CountDownLatch(1);
  private static final AtomicBoolean isInitialized = new AtomicBoolean();
  private static final Log LOG = LogFactory.getLog(HDS.class);
  private static ConcurrentMap<Protocol, DataStorage> dataStorage;
  private static GlobalLockManager lockManager;

  private HDS() {
  }

  public static void initialize(final Configuration config)
      throws Exception {
    if (isInitialized.compareAndSet(false, true)) {
      try {
        conf = config;
        dataStorage = new ConcurrentHashMap<>();
        lockManager = GlobalLockManagerFactory
            .createGlobalLockManager(conf);
      } finally {
        waitLatch.countDown();
      }
    } else {
      waitLatch.await();
    }
  }

  public static DataStorage getDataStorage(
      final UriRequest request, final Configuration conf)
      throws IOException {
    Protocol p = request.getProtocol();
    String clzName = p.getClassName().orElseThrow(() -> {
      return new IOException("Not support scheme.");
    });

    DataStorage ds = dataStorage.computeIfAbsent(p, (v) -> {
      try {
        return createDataStorage(clzName, conf);
      } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
        return null;
      }
    });
    if (ds == null) {
      throw new IOException(
          "Failed to construct the data system for scheme:" + p);
    }
    return ds;
  }

  public static long getDataStorageConnNum(Protocol p) {
    DataStorage ds = dataStorage.get(p);
    return (ds == null) ? 0 : ds.getConnectionNum();
  }

  public static long getSharedMemTotal() {
    DataStorage ds = dataStorage.get(Protocol.hds);
    if (ds == null) {
      return 0;
    }
    HdsDataStorage hdsDs = (HdsDataStorage) ds;
    return hdsDs.getSharedMemoryTotal();
  }

  public static long getSharedMemTaken() {
    DataStorage ds = dataStorage.get(Protocol.hds);
    if (ds == null) {
      return 0;
    }
    HdsDataStorage hdsDs = (HdsDataStorage) ds;
    return hdsDs.getSharedMemoryTaken();
  }

  private static DataStorage createDataStorage(
      String className, final Configuration conf)
      throws ClassNotFoundException, NoSuchMethodException,
      InstantiationException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException {
    Class<? extends DataStorage> clz
        = (Class<? extends DataStorage>) Class.forName(className);
    Constructor<? extends DataStorage> cst
        = clz.getConstructor(Configuration.class);
    return cst.newInstance(conf);
  }

  public static Lock getReadLock(UriRequest uriReq) throws IOException {
    return lockManager.getReadLock(uriReq).orElseThrow(
        () -> new BeLockedException(
            uriReq.toString() + HDSConstants.HODING_READ_LOCK));
  }

  public static Lock getWriteLock(UriRequest uriReq) throws IOException {
    return lockManager.getWriteLock(uriReq).orElseThrow(
        () -> new BeLockedException(
            uriReq.toString() + HDSConstants.HODING_WRITE_LOCK));
  }

  public static long getReadLockNum() {
    return lockManager.getReadLockNum();
  }

  public static long getWriteLockNum() {
    return lockManager.getWriteLockNum();
  }

  public static DataRecord delete(UriRequest request, HdsLog hdsLog, boolean isRecursive) throws IOException {
    StopWatch sourceTime = new StopWatch();
    DataStorage ds = getDataStorage(request, conf);
    hdsLog.setGetSourceElapsedTime(sourceTime.getElapsed());
    StopWatch lockTime = new StopWatch();
    Lock lock = getWriteLock(request);
    hdsLog.setGetLockElapsedTime(lockTime.getElapsed());
    StopWatch actionTime = new StopWatch();
    try {
      return ds.delete(request, isRecursive);
    } catch (IOException e) {
      throw e;
    } finally {
      hdsLog.setActionElapsedTime(actionTime.getElapsed());
      StopWatch releaseLockTime = new StopWatch();
      if (lock != null) {
        lock.close();
      }
      hdsLog.setReleaseLockElapsedTime(releaseLockTime.getElapsed());
    }
  }

  private static boolean checkReqHasBatchDeleteArgs(Query query) {
    boolean hasArgs = false;
    if (query.getQueryValue(HDSConstants.HDS_URI_ARG_VERSION, HDSConstants.HDS_VERSION)
            .equals(HDSConstants.HDS_VERSION_V2)) {
      for (String args : HDSConstants.BATCH_DELETE_LEGAL_ARGS_V2) {
        if (query.contains(args)) {
          hasArgs = true;
          break;
        }
      }
      return hasArgs;
    }
    for (String args : HDSConstants.BATCH_DELETE_LEGAL_ARGS) {
      if (query.contains(args)) {
        hasArgs = true;
        break;
      }
    }
    return hasArgs;
  }

  public static CloseableIterator<BatchDeleteRecord> batchDelete(UriRequest request, boolean ifWildcard, HdsLog hdsLog)
      throws IOException {
    if (!checkReqHasBatchDeleteArgs(request.getQuery())) {
      throw new IOException(HDSConstants.INCORRECT_BATCHDELETE_QUERY);
    }
    StopWatch sourceTime = new StopWatch();
    DataStorage ds = getDataStorage(request, conf);
    hdsLog.setGetSourceElapsedTime(sourceTime.getElapsed());
    CloseableIterator<BatchDeleteRecord> it = null;
    StopWatch actionTime = new StopWatch();
    try {
      request.getQuery().put(HDSConstants.DS_URI_ARG_DIRECTORY, "false");
      it = ds.batchDelete(request, ifWildcard);
      return it;
    } catch (Exception ex) {
      HDSUtil.closeWithLog(it, LOG);
      throw ex;
    } finally {
      hdsLog.setActionElapsedTime(actionTime.getElapsed());
    }
  }

  public static CloseableIterator<DataRecord> list(UriRequest request, Query optArg, HdsLog hdsLog)
      throws IOException {
    StopWatch sourceTime = new StopWatch();
    DataStorage ds = getDataStorage(request, conf);
    hdsLog.setGetSourceElapsedTime(sourceTime.getElapsed());
    StopWatch actionTime = new StopWatch();
    CloseableIterator<DataRecord> it = null;
    try {
      it = ds.list(request, optArg);
      return it;
    } catch (Exception ex) {
      HDSUtil.closeWithLog(it, LOG);
      throw ex;
    } finally {
      hdsLog.setActionElapsedTime(actionTime.getElapsed());
    }
  }

  public static void close() {
    try {
      if (lockManager != null) {
        lockManager.close();
      }
    } catch (IOException ex) {
      LOG.error(ex);
    }
    if (dataStorage != null && !dataStorage.isEmpty()) {
      dataStorage.forEach((Protocol p, DataStorage v) -> {
        try {
          v.close();
        } catch (IOException ex) {
          LOG.error(ex);
        }
      });
    }
  }
}
