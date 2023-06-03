package com.dslab.hdsb.core;

import com.dslab.hdsb.distlib.DistProperty;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import static java.util.stream.Collectors.toList;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistPropertyTest {

  public DistPropertyTest() {

  }

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

  @Test
  public void testBuilder() {
    final int reqCount = 10;
    DistProperty.Builder builder = DistProperty.newBuilder(reqCount);
    for (int i = 0; i < reqCount; i++) {
      builder.addConcurrentTreadNum(Distribution.UNIFORM, 1, 10, 0, 0);
      builder.addDestination(Distribution.ZIPF, "des1,des2", 1.2, 0);
      builder.addFileSize(1, 10, Distribution.UNIFORM, 0, 0);
      builder.addSource(Distribution.UNIFORM, "source1,source2", 1.33, 0);
      builder.addLocalSource("/tmp/");
      builder.addReqFrequency(Distribution.UNIFORM, 2, 10, 0, 0);
      builder.addTreadReqNum(Distribution.UNIFORM, 2, 5, 2, 0);
    }
    checkSizeOfDistProperty(builder.build(), reqCount);
    checkSizeOfDistProperty(builder.parallelBuild(), reqCount);
    checkSizeOfDistProperty(builder.parallelBuild(), reqCount);
    checkSizeOfDistProperty(builder.build(), reqCount);
  }

  private void checkSizeOfDistProperty(DistProperty prop, int expectedSize) {
    assertEquals(expectedSize, getSize(prop.getCopyUploadTargets()));
    assertEquals(expectedSize, getSize(prop.getDestinations()));
    assertEquals(expectedSize, getSize(prop.getFileSizes()));
    assertEquals(expectedSize, getSize(prop.getUploadTargets()));
    assertEquals(expectedSize, getSize(prop.getLocals()));
    assertEquals(expectedSize, getSize(prop.getThreadNumbers()));
    assertEquals(expectedSize, getSize(prop.getThreadRequestNumbers()));
  }

  private static int getSize(Iterator iter) {
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      ++count;
    }
    return count;
  }

  @Test
  public void testLambda() {
    List<String> list1 = new ArrayList<>();
    List<String> list2 = new ArrayList<>();
    list1.add("A1");
    list1.add("B1");
    list1.add("C1");
    list2.add("A2");
    list2.add("B2");
    list2.add("C2");
    List<String> l1 = list1.stream().filter(v -> v.startsWith("A")).collect(toList());
    List<String> l2 = list2.stream().filter(v -> v.startsWith("B")).collect(toList());
    Stream.of(l1, l2).flatMap(l -> l.stream()).forEach(System.out::println);
  }

}
