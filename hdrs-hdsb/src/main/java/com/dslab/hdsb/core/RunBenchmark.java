/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.core;

import com.dslab.hdsb.httprequest.HttpUpload;
import com.dslab.hdsb.httprequest.HttpConn;
import com.dslab.hdsb.createFile.*;
import com.dslab.hdsb.createRequest.ListRequestCreator;
import com.dslab.hdsb.createRequest.RequestCreator;
import com.dslab.hdsb.distlib.DistProperty;
import com.dslab.hdsb.httprequest.HttpList;
import com.dslab.hdsb.response.Response;
import com.dslab.hdsb.response.ResponseMonitor;
import com.dslab.hdsb.response.ResponseScheduler;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class RunBenchmark {

  private static final Log LOG
          = LogFactory.getLog(RunBenchmark.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    //load property
    long startTime = System.currentTimeMillis();
    UserInput userIn = new UserInput(args);
    BenchmarkConfig conf = new BenchmarkConfig(userIn.getConfigPath());
    LOG.info("Load conf and create dist info");
    int userNum = conf.get(BenchmarkConfig.USER_NUMBER);
    int requestNum = conf.get(BenchmarkConfig.REQUEST_NUMBER);
    int socketTimeOut = conf.get(BenchmarkConfig.SOCKET_TIMEOUT);
    String logOutputPath = conf.get(BenchmarkConfig.LOG_OUTPUT_PATH);
    String localFilePath = conf.get(BenchmarkConfig.LOCAL_FILE_PATH);
    String access = conf.get(BenchmarkConfig.ACCESS);
    boolean isCreateLocalFile = conf.get(BenchmarkConfig.IS_CREATE_LOCAL_FILE);
    QueueStaging localBlockingQueue = new QueueStaging(requestNum);
    ListStaging uploadsourceList = new ListStaging();
    ListStaging sourceList = new ListStaging();
    ListStaging destList = new ListStaging();
    DistProperty dist = distCreate(conf);
    long endTime = System.currentTimeMillis();
    showTime("create dist time =", startTime, endTime);
    if (!isCreateLocalFile) {//copyUpload in this is to list source filePath
      LOG.info("List source file and create HDS request's source");
      Iterator<String> dests = dist.getDestinations();
      int dID = 0;
      while (dests.hasNext()) {
        String dest = dests.next();
        dID++;
        destList.setList(dest + "t" + dID);
      }
      String list = access.replace("access", "list");
      String[] sourcePath = conf.get(BenchmarkConfig.SOURCE).split(",");
      ListStaging assignSourceQueue = new ListStaging();
      for (String source : sourcePath) {
        String req = null;
        if (!source.startsWith("local")) {
          ListRequestCreator listRC = new ListRequestCreator(list, source);
          req = listRC.createListReq();
          HttpList httpList = new HttpList(req, assignSourceQueue);
          httpList.runListAPI();
        } else {
          File f = new File(source.replace("local://", ""));
          String filePaht = f.getAbsolutePath();
          for (int i = 0; i < f.listFiles().length; i++) {
            File ff = new File(filePaht + f.list()[i]);
            if (!ff.isDirectory()) {
              assignSourceQueue.setList(source + f.list()[i]);
            }
          }

        }
      }
      long listEndTime = System.currentTimeMillis();
      showTime("list time =", endTime, listEndTime);
      int assignSourceQSize = assignSourceQueue.getSize();
      String[] assignSource = new String[assignSourceQSize];
      Iterator<String> assignSourceIt = assignSourceQueue.getIterator();
      for (int i = 0; i < assignSourceQSize; i++) {
        assignSource[i] = assignSourceIt.next();
      }
      while (sourceList.getSize() <= requestNum) {
        Random r = new Random();
        int k = r.nextInt(assignSourceQSize);
        if (k == assignSource.length) {
          k = assignSource.length - 1;
        } else if (k < 0) {
          k = 0;
        }
        String de = assignSource[k];
        sourceList.setList(de);
      }
    }
    if (isCreateLocalFile) {
      LOG.info("Gen local file to " + localFilePath);
      long localStartTime = System.currentTimeMillis();
      File localfile = new File(localFilePath);
      if (!localfile.exists()) {
        localfile.mkdir();
      }
      int ID = 0;
      Iterator<String> locals = dist.getLocals();
      Iterator<Integer> fileSizes = dist.getFileSizes();
      Iterator<String> copyUploadTargets = dist.getCopyUploadTargets();
      Iterator<String> dests = dist.getDestinations();
      while (locals.hasNext()) {
        ID++;
        String local = locals.next();
        FileFactory ff = new FileFactory(fileSizes);
        ff.createFile(FileFactory.FILETYPE.LOCAL).create(local, ID);
        String prefix;
        if (local.endsWith("/")) {
          prefix = "Test";
        } else {
          prefix = "/Test";
        }
        String comSource = "local://" + local + prefix + ID;
        uploadsourceList.setList(comSource);
        localBlockingQueue.setQueue(local + prefix + ID);
        String source = copyUploadTargets.next();
        if (source.endsWith("/")) {
          prefix = "Test";
        } else {
          prefix = "/Test";
        }
        sourceList.setList(source + prefix + ID);
        String dest = dests.next();
        if (dest.endsWith("/")) {
          prefix = "Test";
        } else {
          prefix = "/Test";
        }
        destList.setList(dest + prefix + ID);
      }
      long localEndTime = System.currentTimeMillis();
      showTime("create local file time = ", localStartTime, localEndTime);
      LOG.info("Gen upload req");
      Iterator<String> uploadSources = uploadsourceList.getIterator();
      Iterator<String> uploadTargets = dist.getUploadTargets();
      RequestCreator localReqCreator = new RequestCreator(access, uploadSources,
              uploadTargets, requestNum);
      long uploadEndTime = System.currentTimeMillis();
      showTime("gen upload request time = ", localEndTime, uploadEndTime);
      LOG.info("Upload local file");
      CountDownLatch localLatch = new CountDownLatch(1);
      ExecutorService localExecutor = Executors.newFixedThreadPool(1);
      BlockingQueue<String> localBlckingQueueIt = localBlockingQueue.getIterable();
      BlockingQueue<URL> localReqQueueIt = localReqCreator.getReqQueue();
      HttpUpload conn = new HttpUpload(localBlckingQueueIt,
              localReqQueueIt, requestNum, localLatch);
      localExecutor.execute(conn);
      localLatch.await();
      localExecutor.shutdownNow();
      long uploadFileEndTime = System.currentTimeMillis();
      showTime("Upload request time = ", uploadEndTime, uploadFileEndTime);
    }
    LOG.info("Gen main req");
    long genReqStartTime = System.currentTimeMillis();
    Iterator<String> dests = destList.getIterator();
    Iterator<String> sources = sourceList.getIterator();
    RequestCreator reqCreator = new RequestCreator(access, sources,
            dests, requestNum);
    long genReqEndTime = System.currentTimeMillis();
    showTime("gen main HDS request time = ", genReqStartTime, genReqEndTime);
    LOG.info("Http req to HDS");
    Response res = new Response(requestNum);
    //schedule
    ScheduledExecutorService resScheduler = Executors.newSingleThreadScheduledExecutor();
    resScheduler.scheduleAtFixedRate(new ResponseScheduler(res, requestNum, logOutputPath), 0, 5, TimeUnit.SECONDS);
    //response 
    CountDownLatch schedulerlatch = new CountDownLatch(requestNum);
    ExecutorService scheduler = Executors.newSingleThreadExecutor();
    scheduler.submit(new ResponseMonitor(res, schedulerlatch, requestNum, logOutputPath));
    //N thread per turn and dist of M request per thread
    CountDownLatch latch = new CountDownLatch(requestNum);
    ExecutorService executor = Executors.newFixedThreadPool(userNum);
    int freqSublistFromIndex = 0;
    int freqSublistToIndex = 0;
    Iterator<Integer> threadNums = dist.getThreadNumbers();
    Iterator<Integer> threadreqNums = dist.getThreadRequestNumbers();
    BlockingQueue<URL> reqQ = reqCreator.getReqQueue();
    while (latch.getCount() > 0) {//request all finished,shutdown thread pool
      if (!reqQ.isEmpty() && threadNums.hasNext()) {
        int threadNum = threadNums.next();
        for (int i = 0; i < threadNum; i++) {
          if (latch.getCount() > 0 && !reqQ.isEmpty() && threadreqNums.hasNext()) {
            int reqNumPerThread = threadreqNums.next();
            freqSublistToIndex += reqNumPerThread;
            if (freqSublistToIndex > requestNum) {
              freqSublistToIndex = requestNum;
            }
            executor.execute(new HttpConn(reqQ,
                    dist.getFrequencies(freqSublistFromIndex, freqSublistToIndex),
                    reqNumPerThread, latch, res, socketTimeOut));
            freqSublistFromIndex = freqSublistToIndex;

          }
        }
      }
    }
    latch.await();
    long mainHDSEndTime = System.currentTimeMillis();
    showTime("run HDS request time = ", genReqEndTime, mainHDSEndTime);
    executor.shutdownNow();
    schedulerlatch.await();
    scheduler.shutdownNow();
    scheduler.awaitTermination(1, TimeUnit.DAYS);
    DeleteLocalFile delete = new DeleteLocalFile(localFilePath);
    delete.deleteAll();
    LOG.info("finish!! log output path = " + logOutputPath);
    resScheduler.shutdownNow();
  }

  private static DistProperty distCreate(BenchmarkConfig conf) {
    int requestNum = conf.get(BenchmarkConfig.REQUEST_NUMBER);
    String source = conf.get(BenchmarkConfig.SOURCE);
    String dest = conf.get(BenchmarkConfig.DESTINATION);
    int fileSizeUpper = conf.get(BenchmarkConfig.FILE_SIZE_UPPER);
    int fileSizeLower = conf.get(BenchmarkConfig.FILE_SIZE_LOWER);
    double reqFreqZipExpoent = conf.get(BenchmarkConfig.FREQUENCY_ZIPF_EXPONENT);
    double reqFreqNormalSd = conf.get(BenchmarkConfig.FREQUENCY_NORMAL_SD);
    String localFilePath = conf.get(BenchmarkConfig.LOCAL_FILE_PATH);
    boolean isCreateLocalFile = conf.get(BenchmarkConfig.IS_CREATE_LOCAL_FILE);

    if (!source.endsWith("/")) {
      String tmp = source + "/";
      source = tmp;
    }
    if (!dest.endsWith("/")) {
      String tmp = dest + "/";
      dest = tmp;
    }

    DistProperty.Builder builder = DistProperty.newBuilder(requestNum);
    for (int i = 0; i < requestNum; i++) {
      if (isCreateLocalFile) {
        builder.addLocalSource(localFilePath);
        builder.addFileSize(fileSizeLower, fileSizeUpper, conf.get(BenchmarkConfig.FILE_SIZE_DIST),
                reqFreqZipExpoent, reqFreqNormalSd);
      }
      builder.addSource(conf.get(BenchmarkConfig.SOURCE_DIST), source,
              conf.get(BenchmarkConfig.SOURCE_ZIPF_EXPONENT),
              conf.get(BenchmarkConfig.SOURCE_NORMAL_SD));

      builder.addDestination(conf.get(BenchmarkConfig.DESTINATION_DIST), dest,
              conf.get(BenchmarkConfig.DESTINATION_ZIPF_EXPONENT),
              conf.get(BenchmarkConfig.DESTINATION_NORMAL_SD));

      builder.addReqFrequency(conf.get(BenchmarkConfig.FREQUENCY_DIST), fileSizeLower, fileSizeUpper,
              conf.get(BenchmarkConfig.FREQUENCY_ZIPF_EXPONENT),
              conf.get(BenchmarkConfig.FREQUENCY_NORMAL_SD));

      builder.addConcurrentTreadNum(conf.get(BenchmarkConfig.CONCURRENT_THREAD_NUMBER_DIST),
              conf.get(BenchmarkConfig.CONCURRENT_THREAD_NUMBER_LOWER),
              conf.get(BenchmarkConfig.CONCURRENT_THREAD_NUMBER_UPPER),
              conf.get(BenchmarkConfig.CONCURRENT_THREAD_NUMBER_ZIPF_EXPONENT),
              conf.get(BenchmarkConfig.CONCURRENT_THREAD_NUMBER_NORMAL_SD));

      builder.addTreadReqNum(conf.get(BenchmarkConfig.THREAD_REQUEST_NUMBER_DIST),
              conf.get(BenchmarkConfig.THREAD_REQUEST_NUMBER_LOWER),
              conf.get(BenchmarkConfig.THREAD_REQUEST_NUMBER_UPPER),
              conf.get(BenchmarkConfig.THREAD_REQUEST_NUMBER_ZIPF_EXPONENT),
              conf.get(BenchmarkConfig.THREAD_REQUEST_NUMBER_NORMAL_SD));
    }
    return builder.parallelBuild();
  }

  private static void showTime(String s, long startTime, long endTime) {
    LOG.info(s + (endTime - startTime) + " ms");
  }
}
