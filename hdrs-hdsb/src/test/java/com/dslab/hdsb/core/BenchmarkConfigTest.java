package com.dslab.hdsb.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


public class BenchmarkConfigTest {
  
  public BenchmarkConfigTest() {
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

  /**
   * The method shouldn't fail, and the number of returned arguments should
   * be bigger than zero.
   */
  @Test
  public void testGetAllArguments() {
    List<Argument> result = BenchmarkConfig.getAllArguments();
    assertNotNull(result);
    assertNotEquals(0, result.size());
  }

  @Test
  public void testGet() throws IOException {
    Argument<Integer> arg0 = Argument.optional("key_v0", 123, Integer::valueOf);
    Argument<String> arg1 = Argument.optional("key_v1", "value", v -> v);
    Argument<Distribution> arg2 = Argument.optional("key_v2", Distribution.NORMAL,
      v -> Distribution.valueOf(v.toUpperCase()));
    Argument<String> arg3 = Argument.required("key_v3", v -> v);
    String arg3Value = "arg3_value";
    assertEquals(123, (int) arg0.getDefaultValue().get());
    assertEquals("value", arg1.getDefaultValue().get());
    assertEquals(Distribution.NORMAL, arg2.getDefaultValue().get());
    Argument<String> failArg = Argument.required("asdasd", v -> v);
    List<Argument> args = new ArrayList<>();
    args.add(arg0);
    args.add(arg1);
    args.add(arg2);
    File propFile = createPropertiesFile(args, arg3, arg3Value);
    args.add(arg3);
    BenchmarkConfig config = new BenchmarkConfig(propFile.getAbsolutePath(), args);
    assertEquals(arg0.getDefaultValue().get(), config.get(arg0));
    assertEquals(arg1.getDefaultValue().get(), config.get(arg1));
    assertEquals(arg2.getDefaultValue().get(), config.get(arg2));
    assertEquals(arg3Value, config.get(arg3));
    try {
      config.get(failArg);
      fail("The " + failArg + " isn't exist so it should fail when getting the argument");
    } catch (IllegalArgumentException e) {
    }
    propFile.delete();
  }

  private static File createPropertiesFile(List<Argument> args, Argument<String> required, String value) throws IOException {
    Properties prop = new Properties();
    args.forEach(v -> prop.put(v.getKey(), v.getDefaultValue().get().toString()));
    prop.put(required.getKey(), value);
    File f = File.createTempFile("-test-properties", null);
    try (FileWriter writer = new FileWriter(f)) {
      prop.store(writer, null);
    }
    return f;
  }
}
