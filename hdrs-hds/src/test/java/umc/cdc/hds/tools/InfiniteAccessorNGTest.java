/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.tools;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author brandboat
 */
public class InfiniteAccessorNGTest {

  public final String testStr = "brandboat isn't a clean coder yet, "
      + "but he wish he can write clean code someday.";

  public InfiniteAccessorNGTest() {
  }

  @Test
  public void inMemTest() throws IOException {
    InfiniteOutputStream.InfiniteDumpLocation ifd = new InfiniteOutputStream.LocalFile();
    InfiniteOutputStream ios = new InfiniteOutputStream(1024, ifd);
    ios.write(testStr.getBytes());
    ios.close();
    InputStream is = ios.getInputStream();
    byte[] readBuffer = new byte[1024];
    int realSize = is.read(readBuffer);
    Assert.assertEquals("In Mem: ", testStr, new String(readBuffer, 0, realSize));
    is.close();
  }

  @Test
  public void inDiskTest() throws IOException {
    InfiniteOutputStream.InfiniteDumpLocation ifd = new InfiniteOutputStream.LocalFile();
    InfiniteOutputStream ios = new InfiniteOutputStream(-1, ifd);
    ios.write(testStr.getBytes());
    ios.close();
    InputStream is = ios.getInputStream();
    byte[] readBuffer = new byte[1024];
    int realSize = is.read(readBuffer);
    is.close();
    ifd.delete();
    Assert.assertEquals("In Mem: ", testStr, new String(readBuffer, 0, realSize));
  }

  @Test
  public void inMemThenDiskTest() throws IOException {
    InfiniteOutputStream.InfiniteDumpLocation ifd = new InfiniteOutputStream.LocalFile();
    InfiniteOutputStream ios = new InfiniteOutputStream(10, ifd);
    ByteArrayInputStream bai = new ByteArrayInputStream(testStr.getBytes());
    int i = 0;
    while ((i = bai.read()) != -1) {
      ios.write((byte) i);
    }
    bai.close();
    ios.close();
    InputStream is = ios.getInputStream();
    byte[] readBuffer = new byte[1024];
    int realSize = is.read(readBuffer);
    is.close();
    ifd.delete();
    Assert.assertEquals("In Mem: ", testStr, new String(readBuffer, 0, realSize));
  }
}
