package com.dslab.drs.simulater.generator;

import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kh87313
 */
public class FilesGeneratorTest {

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of setBound method, of class FilesGenerator.
   */
  @Test
  public void testFilesGenerator() {
    int lowerBound = 5;
    int upperBound = 15;
    NumberListGenerator filesGernerator = new NumberListGenerator(lowerBound, upperBound);

    for (int i = 0; i < 10; i++) {
      List<Integer> files = filesGernerator.nextValue();
      if (files.isEmpty()) {
        fail("There has no fileNumber generated.");
      }
      for (Integer fileNumber : files) {
        if (fileNumber < 1 || fileNumber > upperBound) {
          fail("Has not allow value " + fileNumber);
        }
      }
    }
  }

}
