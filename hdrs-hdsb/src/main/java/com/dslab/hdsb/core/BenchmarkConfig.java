package com.dslab.hdsb.core;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BenchmarkConfig {

  public static final Argument<String> ACCESS = Argument.required("access", v -> v);
  public static final Argument<Integer> USER_NUMBER = Argument.optional("thread.Num", 10, Integer::valueOf);
  public static final Argument<Integer> REQUEST_NUMBER = Argument.optional("request.Num", 100, Integer::valueOf);
  public static final Argument<String> LOG_OUTPUT_PATH = Argument.optional("log.path", "/tmp/", v -> v);
  public static final Argument<String> LOCAL_FILE_PATH = Argument.required("localFile.Path", v -> v);
  public static final Argument<Boolean> IS_CREATE_LOCAL_FILE
          = Argument.optional("isCreateLocalFile", false, Boolean::valueOf);
  public static final Argument<Integer> SOCKET_TIMEOUT = Argument.optional("socket.TimeOut", 60000, Integer::valueOf);

  // Frequence setting.
  public static final Argument<Distribution> FREQUENCY_DIST
          = Argument.optional("frequency.Dist", Distribution.UNIFORM, v -> Distribution.valueOf(v.toUpperCase()));
  public static final Argument<Integer> FREQUENCY_UPPER
          = Argument.optional("frequency.Upper", 100, Integer::valueOf);
  public static final Argument<Integer> FREQUENCY_LOWER
          = Argument.optional("frequency.Lower", 10, Integer::valueOf);
  public static final Argument<Double> FREQUENCY_ZIPF_EXPONENT
          = Argument.optional("frequency.Zipf.Exponent", 2.0, Double::valueOf);
  public static final Argument<Double> FREQUENCY_NORMAL_SD
          = Argument.optional("frequency.Normal.SD", 1.0, Double::valueOf);

  // File size setting.
  public static final Argument<Integer> FILE_SIZE_UPPER
          = Argument.optional("localFile.fileSize.Upper", 100, Integer::valueOf);
  public static final Argument<Integer> FILE_SIZE_LOWER
          = Argument.optional("localFile.fileSize.Lower", 1, Integer::valueOf);
  public static final Argument<Distribution> FILE_SIZE_DIST
          = Argument.optional("localFile.fileSize.Dist", Distribution.UNIFORM, v -> Distribution.valueOf(v.toUpperCase()));
  public static final Argument<Double> FILE_SIZE_ZIPF_EXPONENT
          = Argument.optional("localFile.fileSize.Zipf.Exponent", 2.0, Double::valueOf);
  public static final Argument<Double> FILE_SIZE_NORMAL_SD
          = Argument.optional("localFile.fileSize.Normal.SD", 1.0, Double::valueOf);

  // Source setting.
  public static final Argument<String> SOURCE = Argument.required("source", v -> v);
  public static final Argument<Distribution> SOURCE_DIST
          = Argument.optional("source.Dist", Distribution.UNIFORM, v -> Distribution.valueOf(v.toUpperCase()));
  public static final Argument<Double> SOURCE_ZIPF_EXPONENT
          = Argument.optional("source.Zipf.Exponent", 2.0, Double::valueOf);
  public static final Argument<Double> SOURCE_NORMAL_SD
          = Argument.optional("source.Normal.SD", 1.0, Double::valueOf);

  // Destination setting.
  public static final Argument<String> DESTINATION = Argument.required("destination", v -> v);
  public static final Argument<Distribution> DESTINATION_DIST
          = Argument.optional("destination..Dist", Distribution.UNIFORM, v -> Distribution.valueOf(v.toUpperCase()));
  public static final Argument<Double> DESTINATION_ZIPF_EXPONENT
          = Argument.optional("destination.Zipf.Exponent", 2.0, Double::valueOf);
  public static final Argument<Double> DESTINATION_NORMAL_SD
          = Argument.optional("destination.Normal.SD", 1.0, Double::valueOf);

  //Thread's request number setting.
  public static final Argument<Distribution> THREAD_REQUEST_NUMBER_DIST
          = Argument.optional("threadRequestNum.Dist", Distribution.UNIFORM, v -> Distribution.valueOf(v.toUpperCase()));
  public static final Argument<Integer> THREAD_REQUEST_NUMBER_UPPER
          = Argument.optional("threadRequestNum.Upper", 10, Integer::valueOf);
  public static final Argument<Integer> THREAD_REQUEST_NUMBER_LOWER
          = Argument.optional("threadRequestNum.Lower", 1, Integer::valueOf);
  public static final Argument<Double> THREAD_REQUEST_NUMBER_ZIPF_EXPONENT
          = Argument.optional("threadRequestNum.Zipf.Exponent", 2.0, Double::valueOf);
  public static final Argument<Double> THREAD_REQUEST_NUMBER_NORMAL_SD
          = Argument.optional("threadRequestNum.Normal.SD", 1.0, Double::valueOf);

  //Concurrent thread number setting 
  public static final Argument<Distribution> CONCURRENT_THREAD_NUMBER_DIST
          = Argument.optional("concurrentThreadNum.Dist", Distribution.UNIFORM, v -> Distribution.valueOf(v.toUpperCase()));
  public static final Argument<Integer> CONCURRENT_THREAD_NUMBER_UPPER
          = Argument.optional("concurrentThreadNum.Upper", 5, Integer::valueOf);
  public static final Argument<Integer> CONCURRENT_THREAD_NUMBER_LOWER
          = Argument.optional("concurrentThreadNum.Lower", 1, Integer::valueOf);
  public static final Argument<Double> CONCURRENT_THREAD_NUMBER_ZIPF_EXPONENT
          = Argument.optional("concurrentThreadNum.Zipf.Exponent", 2.0, Double::valueOf);
  public static final Argument<Double> CONCURRENT_THREAD_NUMBER_NORMAL_SD
          = Argument.optional("concurrentThreadNum.Normal.SD", 1.0, Double::valueOf);

  private final Properties props = new Properties();

  public BenchmarkConfig(String confPath) throws IOException {
    this(confPath, getAllArguments());
  }

  @VisibleForTesting
  BenchmarkConfig(String confPath, List<Argument> args) throws IOException {
    Path confLocation = Paths.get(confPath);
    try (InputStream stream = Files.newInputStream(confLocation)) {
      props.load(stream);
    }
    checkOptions(args);
  }

  private void checkOptions(List<Argument> args) {
    args.forEach(v -> v.check(props));
  }

  @VisibleForTesting
  static List<Argument> getAllArguments() {
    List<Argument> args = new ArrayList<>();
    for (Field f : BenchmarkConfig.class.getFields()) {
      try {
        Object o = f.get(BenchmarkConfig.class);
        if (Argument.class.isAssignableFrom(o.getClass())) {
          args.add((Argument) o);
        }
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex);
      }
    }
    return args;
  }

  public <T> T get(Argument<T> option) {
    return option.get(props);
  }

}
