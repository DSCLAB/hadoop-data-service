/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import umc.cdc.hds.exceptions.ReadQueueIsEmptyException;

/**
 *
 * @author jpopaholic
 */
public class BlockingWritableInputStreamNGTest {

  String[] strings = {"this is a pen,", "and that is an apple", "."};

  public BlockingWritableInputStreamNGTest() {

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
   * Test of read method, of class BlockingWritableInputStream.
   */
  @Test
  public void testRead() throws Exception {
    System.out.println("read");
    BlockingWritableInputStream instance = new BlockingWritableInputStream(2, 5);
    int result = 0;
    try {
      result = instance.read();
    } catch (ReadQueueIsEmptyException ex) {
      instance.append(strings[0]);
      instance.append(strings[1]);
      instance.append(strings[2]);
      instance.setNoDataComing();
      result = instance.read();
    }
    assertEquals(result, 't');
  }

  @Test
  public void testReadByte() throws Exception {
    System.out.println("read long");
    BlockingWritableInputStream instance = new BlockingWritableInputStream(2, 5);
    byte[] buffer = new byte[3];
    StringBuilder builder = new StringBuilder();
    int readByte = 1;
    int i = 0;
    do {
      try {
        readByte = instance.read(buffer);
        if (readByte == -1) {
          break;
        }
        builder.append(Bytes.toString(buffer, 0, readByte));
      } catch (ReadQueueIsEmptyException ex) {
        try {
          instance.append(strings[i]);
          i++;
        } catch (ArrayIndexOutOfBoundsException ie) {
          instance.setNoDataComing();
        }
      }
    } while (readByte > 0);
    assertEquals(builder.toString(), "this is a pen,and that is an apple.");
  }

  @Test
  public void testReadEndCarriageReturn() throws IOException {
    System.out.println("read carriageReturn");
    BlockingWritableInputStream instance = new BlockingWritableInputStream(2, 5);
    instance.append("thi\r");
    instance.append("\nis aaa.");
    byte[] result = new byte[5];
    StringBuilder builder = new StringBuilder();
    int readByte;
    try {
      while ((readByte = instance.read(result)) > 0) {
        builder.append(Bytes.toString(result, 0, readByte));
      }
    } catch (EOFException ex) {

    }
    System.out.println(builder.toString());
  }
}
