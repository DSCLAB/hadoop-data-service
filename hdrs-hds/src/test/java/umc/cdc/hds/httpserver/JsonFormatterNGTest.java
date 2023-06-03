/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.ws.rs.core.Response.Status;
import org.testng.annotations.Test;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.datastorage.status.DataRecord;
import umc.cdc.hds.datastorage.status.DataRecordBuilder;

/**
 *
 * @author brandboat
 */
public class JsonFormatterNGTest {

  private JsonFormatter jsonFormatter;
  private ByteArrayOutputStream buffer;
  private final DataRecord dr;

  public JsonFormatterNGTest() throws IOException {
    Map<String, Double> dataOwner = new TreeMap<>();
    dataOwner.put("brandboat", 0.1);
    dataOwner.put("brandboat2", 0.2);
    dr = new DataRecordBuilder()
        .setName("brandboat")
        .setProtocol(Protocol.hds)
        .setFileType(DataRecord.FileType.file)
        .setSize(0)
        .setUri("hds:///vds")
        .setOwner(dataOwner)
        .setUploadTime(System.currentTimeMillis()).build();
  }

  @Test
  public void testConvertListElement() throws IOException {
    initializeFormatter();
    jsonFormatter.convert(new Result(Status.OK, dr, HDSConstants.LIST_DATAINFO));
    jsonFormatter.close();
    System.out.println(new String(buffer.toByteArray()));
  }

  @Test
  public void testConvertIteratorListElement() throws IOException {
    initializeFormatter();
    List<DataRecord> ldr = new ArrayList<>();
    ldr.add(dr);
    ldr.add(dr);
    jsonFormatter.convert(new Result(Status.OK, ldr.iterator(), HDSConstants.LIST_DATAINFO));
    jsonFormatter.close();
    System.out.println(new String(buffer.toByteArray()));
  }

  private void initializeFormatter() throws IOException {
    buffer = new ByteArrayOutputStream(1024 * 1024);
    jsonFormatter = new JsonFormatter(buffer);
  }
}
