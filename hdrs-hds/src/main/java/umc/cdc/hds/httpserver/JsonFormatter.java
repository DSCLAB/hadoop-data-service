/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import com.dslab.drs.api.ApplicationStatus;
import com.dslab.drs.api.KilldrsResponseMessage;
import com.dslab.drs.api.NodeStatus;
import com.dslab.drs.api.ServiceStatus;
import com.dslab.drs.tasks.FileInfo;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.datastorage.status.BatchDeleteRecord;
import umc.cdc.hds.datastorage.status.ErrorInfo;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.filter.DatabaseInfo;
import umc.cdc.hds.task.TaskInfoBuilder.TaskInfo;
import umc.cdc.hds.load.NodeLoadInfo;
import umc.cdc.hds.load.OperationLoadInfo;
import umc.cdc.hds.mapping.AccountInfo;
import umc.cdc.hds.tools.CloseableIterator;

/**
 *
 * @author brandboat
 */
public final class JsonFormatter implements Formatter {

  private final JsonWriter jw;

  public JsonFormatter(OutputStream os)
      throws UnsupportedEncodingException, IOException {
    OutputStreamWriter osw = new OutputStreamWriter(os);
    jw = new JsonWriter(osw);
  }

  @Override
  public void convert(Result result) throws IOException {
    Object obj = result.getContent();
    String head = result.getTitle();
    if (obj instanceof Iterator) {
      if (obj instanceof CloseableIterator) {
        CloseableIterator<Object> it = (CloseableIterator<Object>) obj;
        serializeCloseableIterator(it, head);
      } else {
        Iterator<Object> it = (Iterator<Object>) obj;
        serializeIterator(it, head);
      }
    } else {
      serialize(obj, head);
    }
  }

  /**
   * JsonWriter will write an "Empty Document" when it has been Initialized, if
   * you doesn't write anything and just close. It will throw "Incomplete
   * Document" Exception. But I need it do alright to be initialized and then be
   * closed. So this close will not throw any IOException.
   */
  @Override
  public void close() {
    IOUtils.closeQuietly(jw);
  }

  private void serializeIterator(
      Iterator<Object> it, String head) throws IOException {
    jw.beginObject().name(head).beginArray();
    while (it.hasNext()) {
      serialize(it.next());
    }
    jw.endArray().endObject();
  }

  private void serializeCloseableIterator(
      CloseableIterator<Object> it, String head) throws IOException {
    try {
      serializeIterator(it, head);
    } catch (Exception ex) {
      throw ex;
    } finally {
      it.close();
    }
  }

  private void serialize(Object obj, String head) throws IOException {
    jw.beginObject();
    jw.name(head);
    serialize(obj);
    jw.endObject();
  }

  private void serialize(Object obj) throws IOException {
    if (obj instanceof DataRecord) {
      if (obj instanceof BatchDeleteRecord) {
        serializeBatchDeleteRecord((BatchDeleteRecord) obj);
        return;
      }
      serializeDataRecord((DataRecord) obj);
    }
    if (obj instanceof ErrorInfo) {
      serializeErrorDetail((ErrorInfo) obj);
    }
    if (obj instanceof TaskInfo) {
      serializeTask((TaskInfo) obj);
    }
    if (obj instanceof AccountInfo) {
      serializeAccountInfo((AccountInfo) obj);
    }
    if (obj instanceof ServiceStatus) {
      serializeServiceStatus((ServiceStatus) obj);
    }
    if (obj instanceof ApplicationStatus) {
      serializeApplicationStatus((ApplicationStatus) obj);
    }
    if (obj instanceof NodeLoadInfo) {
      serializeNodeLoadInfo((NodeLoadInfo) obj);
    }
    if (obj instanceof KilldrsResponseMessage) {
      serializeKilldrsResponseMessage((KilldrsResponseMessage) obj);
    }
    if (obj instanceof DatabaseInfo) {
      serializeDatabaseInfo((DatabaseInfo) obj);
      /*String res = (String)obj;
      String[] args = res.split("column name :");
      if (args.length == 1) {
        jw.beginObject();
        jw.name("Table name").value(args[0]);
        jw.endObject();
      }else if (args.length == 2) {
        jw.beginObject();
        jw.name("Table name").value(args[0]);
        jw.name("Column name").value(args[1]);
        jw.endObject();
      }*/


    }
  }
  private void serializeDatabaseInfo(DatabaseInfo st) throws IOException {
    jw.beginObject();
    internalSerializeDatabaseInfo(st);
    jw.endObject();
  }
  private void internalSerializeDatabaseInfo(DatabaseInfo di) throws IOException {
    jw.name("Table name").value(di.getTableName());
    if(di.getKeyName()!=null) {
      jw.name("Column name").value(di.getKeyName());
    }
  }
  private void serializeDataRecord(DataRecord le) throws IOException {
    jw.beginObject();
    internalSerializeDataRecord(le);
    jw.endObject();
  }

  private void serializeDataOwner(Map<String, Double> dataOwner)
      throws IOException {
    jw.name(HDSConstants.LIST_DATAOWNER);
    jw.beginArray();
    for (Map.Entry<String, Double> entry : dataOwner.entrySet()) {
      jw.beginObject();
      jw.name(HDSConstants.LIST_DATAOWNER_HOST).value(entry.getKey());
      jw.name(HDSConstants.LIST_DATAOWNER_RATIO).value(
          formattingRatio(entry.getValue()));
      jw.endObject();
    }
    jw.endArray();
  }

  private void serializeBatchDeleteRecord(BatchDeleteRecord bd) throws IOException {
    jw.beginObject();
    internalSerializeDataRecord(bd);
    jw.name(HDSConstants.LIST_ERROR_MESSAGE).value(bd.getErrorMessage());
    jw.endObject();
  }

  private void internalSerializeDataRecord(DataRecord dr) throws IOException {
    jw.name(HDSConstants.LIST_URI).value(dr.getUri());
    jw.name(HDSConstants.LIST_LOCATION).value(dr.getLocation().name());
    jw.name(HDSConstants.LIST_NAME).value(dr.getName());
    jw.name(HDSConstants.LIST_SIZE).value(dr.getSize());
    jw.name(HDSConstants.LIST_TIME).value(formattingTime(dr.getTime()));
    jw.name(HDSConstants.LIST_FILETYPE).value(dr.getFileType().map((DataRecord.FileType v) -> {
      if (v == null) {
        return null;
      }
      return v.name();
    }).get());
    serializeDataOwner(dr.getDataOwner());
  }

  private void serializeErrorDetail(ErrorInfo error) throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.ERROR_RESPONSE);
    jw.beginObject();
    jw.name(HDSConstants.ERROR_RESPONSE_JAVACLASSNAME)
        .value(error.getClassName());
    jw.name(HDSConstants.ERROR_RESPONSE_EXCEPTION)
        .value(error.getExceptionName());
    jw.name(HDSConstants.ERROR_RESPONSE_MESSAGE)
        .value(error.getErrorMessage());
    jw.endObject();
    jw.endObject();
  }

  private void serializeTask(TaskInfo task) throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.TASK_ID).value(task.getId());
    jw.name(HDSConstants.TASK_REDIRECT_FROM).value(task.getRedirectFrom());
    jw.name(HDSConstants.TASK_SERVER_NAME).value(task.getServerName());
    jw.name(HDSConstants.TASK_CLIENT_NAME).value(task.getClientName());
    jw.name(HDSConstants.TASK_FROM).value(task.getFrom());
    jw.name(HDSConstants.TASK_TO).value(task.getTo());
    jw.name(HDSConstants.TASK_STATE).value(task.getStateEnum().name());
    jw.name(HDSConstants.TASK_PROGRESS).value(formattingRatio(task.getProgress()));
    jw.name(HDSConstants.TASK_START_TIME).value(formattingTime(task.getStartTime()));
    jw.name(HDSConstants.TASK_ELAPSED).value(task.getElapsedTime());
    jw.name(HDSConstants.TASK_EXPECTED_SIZE).value(task.getDataSize());
    jw.name(HDSConstants.TASK_TRANSFERRED_SIZE).value(task.getDataTransferSize());
    jw.endObject();
  }

  private void serializeKilldrsResponseMessage(KilldrsResponseMessage res)
      throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.KILL_DRS_MESSAGE).value(res.getKillDrsResponse());
    jw.endObject();
  }

  private void serializeAccountInfo(AccountInfo ai) throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.MAPPING_ARG_ID).value(ai.getId().orElse(null));
    jw.name(HDSConstants.MAPPING_ARG_DOMAIN).value(ai.getDomain().orElse(null));
    jw.name(HDSConstants.MAPPING_ARG_USER).value(ai.getUser().orElse(null));
    jw.name(HDSConstants.MAPPING_ARG_PASSWD).value(ai.getPasswd().orElse(null));
    jw.name(HDSConstants.MAPPING_ARG_HOST).value(ai.getHost().orElse(null));
    jw.name(HDSConstants.MAPPING_ARG_PORT).value(ai.getPort().orElse(null));
    jw.endObject();
  }

  private void serializeServiceStatus(ServiceStatus serviceStatus)
      throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.RUN_RESPONSE_ID)
        .value(serviceStatus.getApplicationID());
    jw.name(HDSConstants.RUN_RESPONSE_AMNODE)
        .value(serviceStatus.getAMnode());
    jw.name(HDSConstants.RUN_RESPONSE_CODE)
        .value(serviceStatus.getCode());
    jw.name(HDSConstants.RUN_RESPONSE_DATA)
        .value(serviceStatus.getData());
    jw.name(HDSConstants.RUN_RESPONSE_CONFIG)
        .value(serviceStatus.getConfig());
    jw.name(HDSConstants.RUN_RESPONSE_COPYTO)
        .value(serviceStatus.getCopyto());
    jw.name(HDSConstants.RUN_RESPONSE_STATUS)
        .value(serviceStatus.getStatus());
    jw.name(HDSConstants.RUN_RESPONSE_ERROR_MESSAGE)
        .value(serviceStatus.getErrorMessage());
    jw.name(HDSConstants.RUN_RESPONSE_IS_COMPLETE)
        .value(serviceStatus.getIsComplete());
    jw.name(HDSConstants.RUN_RESPONSE_STARTTIME)
        .value(serviceStatus.getStartTime());
    jw.name(HDSConstants.RUN_RESPONSE_ELAPSED)
        .value(serviceStatus.getElapsed());
    jw.name(HDSConstants.RUN_RESPONSE_FILECOUNT)
        .value(serviceStatus.getFileCount());
    jw.name(HDSConstants.RUN_RESPONSE_PROGRESS)
        .value(serviceStatus.getProgress());
    jw.name(HDSConstants.RUN_RESPONSE_HDSFILEINFOCOLLECTTIMESMS)
        .value(serviceStatus.getHdsFileInfoCollectTimeMs());

    // begin serialize node status
    jw.name(HDSConstants.RUN_RESPONSE_NODESSTATUS).beginArray();
    Iterator<NodeStatus> itNodeStatus
        = serviceStatus.getNodesStatus().values().iterator();
    while (itNodeStatus.hasNext()) {
      NodeStatus nodeStatus = itNodeStatus.next();
      jw.beginObject();
      jw.name(HDSConstants.CONTAINER_INFO_CONTAINERID)
          .value(nodeStatus.getContainerID());
      jw.name(HDSConstants.CONTAINER_INFO_ISCLOSE)
          .value(nodeStatus.getIsClosed());
      jw.name(HDSConstants.CONTAINER_INFO_NODENAME)
          .value(nodeStatus.getNodeName());
      jw.name(HDSConstants.CONTAINER_INFO_WORKINGTASK)
          .value(nodeStatus.getWorkingTask());
      jw.name(HDSConstants.CONTAINER_INFO_STARTTIME)
          .value(nodeStatus.getStartTime());
      jw.name(HDSConstants.CONTAINER_INFO_ENDTIME)
          .value(nodeStatus.getEndTime());
      jw.name(HDSConstants.CONTAINER_INFO_STATUS)
          .value(nodeStatus.getStatus());
      jw.name(HDSConstants.CONTAINER_INFO_CONTAINERINTERVAL)
              .value(nodeStatus.getContainerInterval());
      jw.name(HDSConstants.CONTAINER_INFO_MEMSIZE)
              .value(nodeStatus.getMemSize());
      // begin serialize node done task
      jw.name(HDSConstants.CONTAINER_INFO_NODEDONETASK).beginArray();
      for (String doneTask : nodeStatus.getNodeDoneTask()) {
        jw.value(doneTask);
      }
      jw.endArray();
      // begin serialize node fail task
      jw.name(HDSConstants.CONTAINER_INFO_NODEFAILTASK).beginArray();
      for (String failTask : nodeStatus.getNodeFailTask()) {
        jw.value(failTask);
      }
      jw.endArray();
      jw.endObject();
    }
    jw.endArray();

    // begin serialize task info.
    jw.name(HDSConstants.RUN_RESPONSE_TASK).beginArray();
    if (serviceStatus.getTasks() != null) {
      for (FileInfo task : serviceStatus.getTasks()) {
        jw.beginObject();
        jw.name(HDSConstants.TASK_URL).value(task.getUrl());
        jw.name(HDSConstants.TASK_SIZE).value(task.getSize());
        jw.name(HDSConstants.TASK_STATUS).value(task.getTaskStatus());
        jw.endObject();
      }
    }
    jw.endArray();
    jw.endObject();
  }

  private void serializeApplicationStatus(ApplicationStatus as)
      throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.APPLICATION_ID)
        .value(as.getApplicationID());
    jw.name(HDSConstants.APPLICATION_AMNODE)
        .value(as.getAMnode());
    jw.name(HDSConstants.APPLICATION_STATUS)
        .value(as.getStatus());
    jw.name(HDSConstants.APPLICATION_CODE)
        .value(as.getCode());
    jw.name(HDSConstants.APPLICATION_DATA)
        .value(as.getData());
    jw.name(HDSConstants.APPLICATION_CONFIG)
        .value(as.getConfig());
    jw.name(HDSConstants.APPLICATION_COPYTO)
        .value(as.getCopyto());
    jw.name(HDSConstants.APPLICATION_FILECOUNT)
        .value(as.getFileCount());
    jw.name(HDSConstants.APPLICATION_PROGRESS)
        .value(as.getProgress());
    jw.name(HDSConstants.APPLICATION_ELAPSED)
        .value(as.getElapsed());
    jw.name(HDSConstants.APPLICATION_USED_CONTAINERS)
        .value(as.getUsedContainers());
    jw.name(HDSConstants.APPLICATION_USED_VIRTUAL_MEMORY)
        .value(as.getUsedVirtualMemory());
    jw.name(HDSConstants.APPLICATION_USED_VIRTUAL_CORES)
        .value(as.getUsedVirtualCores());
    jw.endObject();
  }

  private void serializeNodeLoadInfo(NodeLoadInfo nodeInfo)
      throws IOException {
    jw.beginObject();
    jw.name(HDSConstants.NODE_LOAD_HOST)
        .value(nodeInfo.getHost());
    jw.name(HDSConstants.NODE_LOAD_PORT)
        .value(nodeInfo.getPort());
    jw.name(HDSConstants.NODE_LOAD_OPERATION);
    jw.beginArray();
    for (OperationLoadInfo opInfo : nodeInfo.getOperationLoadInfos()) {
      jw.beginObject();
      jw.name(HDSConstants.LOAD_METHOD)
          .value(opInfo.getLoadingCategory().name());
      jw.name(HDSConstants.LOAD_DEAL_COUNT)
          .value(opInfo.getDealCount());
      jw.name(HDSConstants.LOAD_DEAL_BYTES)
          .value(opInfo.getDealBytes());
      jw.name(HDSConstants.LOAD_PAST_COUNT)
          .value(opInfo.getPastCount());
      jw.name(HDSConstants.LOAD_PAST_BYTES)
          .value(opInfo.getPastBytes());
      jw.endObject();
    }
    jw.endArray();
    jw.endObject();
  }

  private static double formattingRatio(double ratio) {
    DecimalFormat df = new DecimalFormat("#.0");
    Double r = Double.valueOf(df.format(ratio));
    return r;
  }

  private static String formattingTime(long time) {
    DateTimeFormatter fmt = DateTimeFormat.forPattern(
        HDSConstants.DEFAULT_TIME_FORMAT);
    DateTimeZone dtz = DateTimeZone.forID(
        HDSConstants.DEFAULT_TIME_ZONE);
    DateTime dt = new DateTime(time, dtz);
    return fmt.print(dt);
  }
}
