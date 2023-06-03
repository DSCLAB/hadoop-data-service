/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord.FileType;

/**
 *
 * @author jpopaholic
 */
public class BaseDataRecordWrapperNGTest {

  private final FInfo[] files;

  public BaseDataRecordWrapperNGTest() {
    Map<String, Double> allowner = new TreeMap();
    allowner.put("localhost", 1.0);
    files = new FInfo[3];
    for (int i = 0; i < 3; i++) {
      files[i] = new FInfo();
    }
    files[0].name = "F1";
    files[0].size = 10l;
    files[0].owner = allowner;
    files[0].time = 2000l;
    files[0].type = FileType.file;
    files[0].uri = "file:///test/F1";

    files[1].name = "F2";
    files[1].size = 12l;
    files[1].owner = allowner;
    files[1].time = 3000l;
    files[1].type = FileType.file;
    files[1].uri = "file:///test/F2";

    files[2].name = "F3";
    files[2].size = 13l;
    files[2].owner = allowner;
    files[2].time = 4000l;
    files[2].type = FileType.file;
    files[2].uri = "file:///test/F3";
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

  @BeforeMethod
  public void setUpMethod() throws Exception {
  }

  @AfterMethod
  public void tearDownMethod() throws Exception {
  }

  /**
   * Test of wrapSingleFile method, of class BaseDataRecordWrapper.
   */
  @Test
  public void testWrapSingleFile() {
    System.out.println("wrapSingleFile");
    BaseDataRecordWrapper instance = new BaseDataRecordWrapperImpl();
    DataRecord expResult = null;
    DataRecord result = instance.wrapSingleFile(files[0]);
    assertEquals(result.getName(), files[0].name);
    assertEquals(result.getSize(), files[0].size);
    assertEquals(result.getTime(), files[0].time);
    assertEquals(result.getFileType().get(), FileType.file);
    assertEquals(result.getDataOwner().get("localhost"), 1.0);
    assertEquals(result.getUri(), files[0].uri);
  }

  /**
   * Test of wrapParticalFilesWithErrorMessage method, of class
   * BaseDataRecordWrapper.
   */
  @Test
  public void testWrapParticalFilesWithErrorMessage() {
    System.out.println("wrapParticalFilesWithErrorMessage");
    Map<String, Double> allowner = new TreeMap();
    allowner.put("localhost", 1.0);
    int[] indexs = {0, 2};
    List<Exception> errors = new ArrayList();
    errors.add(new IOException("test error1"));
    errors.add(new FileNotFoundException("test error2"));
    List<BatchDeleteRecord> expResult = new ArrayList();
    BaseDataRecordWrapper instance = new BaseDataRecordWrapperImpl();
    expResult.add(new BatchDeleteRecord(new DataRecord("file:///test/F1", Protocol.file, "F1", 10l, 2000l, FileType.file, allowner),
        new ErrorInfo(new IOException("test error1"))));
    expResult.add(new BatchDeleteRecord(new DataRecord("file:///test/F3", Protocol.file, "F3", 13l, 4000l, FileType.file, allowner),
        new ErrorInfo(new FileNotFoundException("test error2"))));
    Iterator<BatchDeleteRecord> result = instance.wrapParticalFilesWithErrorMessage(files, indexs, errors);
    Iterator<BatchDeleteRecord> exp = expResult.iterator();
    assertEquals(result, exp, "expect: \n" + showBatchDeleteRecord(exp)
        + "\nbut result: \n" + showBatchDeleteRecord(result));
  }

  /**
   * Test of wrapFiles method, of class BaseDataRecordWrapper.
   */
  @Test

  public void testWrapFiles() {
    System.out.println("wrapFiles");
    Map<String, Double> allowner = new TreeMap();
    allowner.put("localhost", 1.0);
    BaseDataRecordWrapper instance = new BaseDataRecordWrapperImpl();
    List<DataRecord> expResult = new ArrayList();
    expResult.add(new DataRecord("file:///test/F1", Protocol.file, "F1", 10l, 2000l, FileType.file, allowner));
    expResult.add(new DataRecord("file:///test/F2", Protocol.file, "F2", 12l, 3000l, FileType.file, allowner));
    expResult.add(new DataRecord("file:///test/F3", Protocol.file, "F3", 13l, 4000l, FileType.file, allowner));
    Iterator<DataRecord> result = instance.wrapFiles(files);
    Iterator<DataRecord> exp = expResult.iterator();
    assertEquals(result, exp, "expect: \n" + showDataRecord(exp)
        + "\nbut result: \n" + showDataRecord(result));
  }

  public class BaseDataRecordWrapperImpl extends BaseDataRecordWrapper<FInfo> {

    @Override
    public String wrapName(FInfo file) {
      return file.name;
    }

    @Override
    public long wrapSize(FInfo file) {
      return file.size;
    }

    @Override
    public long wrapModifiedTime(FInfo file) {
      return file.time;
    }

    @Override
    public Protocol wrapLocation() {
      return Protocol.file;
    }

    @Override
    public Map<String, Double> wrapDataOwner(FInfo file) {
      return file.owner;
    }

    @Override
    public DataRecord.FileType wrapFileType(FInfo file) {
      return file.type;
    }

    @Override
    public String wrapUri(FInfo file) {
      return file.uri;
    }
  }

  public class FInfo {

    public String name;
    public long size;
    public long time;
    public Map<String, Double> owner;
    public FileType type;
    public String uri;
  }

  private static String showDataRecord(Iterator<DataRecord> result) {
    StringBuilder resultContext = new StringBuilder();
    while (result.hasNext()) {
      DataRecord record = result.next();
      resultContext.append("{\n");
      resultContext.append("name: ").append(record.getName()).append(",\n");
      resultContext.append("size: ").append(record.getSize()).append(",\n");
      resultContext.append("time: ").append(record.getTime()).append(",\n");
      resultContext.append("uri: ").append(record.getUri()).append(",\n");
      resultContext.append("location: ").append(record.getLocation().name()).append(",\n");
      resultContext.append("file type: ").append(record.getFileType()).append(",\n");
      resultContext.append("owner: ").append(record.getDataOwner()).append("\n}\n");
    }
    return resultContext.toString();
  }

  private static String showBatchDeleteRecord(Iterator<BatchDeleteRecord> result) {
    StringBuilder resultContext = new StringBuilder();
    while (result.hasNext()) {
      BatchDeleteRecord record = result.next();
      resultContext.append("{\n");
      resultContext.append("name: ").append(record.getName()).append(",\n");
      resultContext.append("size: ").append(record.getSize()).append(",\n");
      resultContext.append("time: ").append(record.getTime()).append(",\n");
      resultContext.append("uri: ").append(record.getUri()).append(",\n");
      resultContext.append("location: ").append(record.getLocation().name()).append(",\n");
      resultContext.append("file type: ").append(record.getFileType()).append(",\n");
      resultContext.append("owner: ").append(record.getDataOwner()).append(",\n");
      resultContext.append("error: ").append(record.getErrorMessage()).append("\n}\n");
    }
    return resultContext.toString();
  }

}
