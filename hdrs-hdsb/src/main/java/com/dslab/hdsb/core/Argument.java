package com.dslab.hdsb.core;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

public class Argument<T> {
  private final String key;
  private final T defaultValue;
  private final Function<String, T> f;

  static <T> Argument<T> required(String key, final Function<String, T> f) {
    return new Argument<>(key, null, f);
  }

  static <T> Argument<T> optional(String key, T defaultValue, final Function<String, T> f) {
    assert defaultValue != null;
    return new Argument<T>(key, defaultValue, f) {
      @Override
      protected void check(Properties property) {
      }
    };
  }

  private Argument(final String key, final T defaultValue, final Function<String, T> f) {
    assert key != null : "The key cann't be null";
    assert f != null : "The transform function cann't be null";
    this.key = key;
    this.defaultValue = defaultValue;
    this.f = f;
  }

  void check(Properties property) {
    if (!property.containsKey(getKey())) {
      throw createException(getKey());
    }
  }

  private static IllegalArgumentException createException(String key) {
    return new IllegalArgumentException("Failed to find the " + key);
  }

  public Optional<T> getDefaultValue() {
    return Optional.ofNullable(defaultValue);
  }

  T get(Properties property) {
    String value = property.getProperty(key);
    if (value == null) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw createException(key);
    } else {
      return f.apply(value);
    }
  }

  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return getKey();
  }
}
