/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.uri;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.exceptions.IllegalUriException;

/**
 *
 * @author jpopaholic
 */
public class Query {

  protected Map<String, String> queries;

  public Query() {
    queries = new TreeMap();
  }

  public Query(String rawQuery) throws UnsupportedEncodingException, IllegalUriException {
    queries = parseQuery(rawQuery);
  }

  public Query(Map<String, String> rawQuery) {
    queries = new TreeMap<>(rawQuery);
  }

  public int getLimit() {
    return parsePositiveInteger(HDSConstants.LIMIT,
            Integer.MAX_VALUE, HDSConstants.DEFAULT_LIST_LIMIT);
  }

  public int getOffset() {
    return parsePositiveInteger(HDSConstants.OFFSET, 0);
  }

  public void put(String key, String value) {
    queries.put(key, value);
  }

  public Map<String, String> getQueries() {
    return queries;
  }

  public Optional<String> getQueryValue(String key) {
    return Optional.ofNullable(queries.get(key));
  }

  public String getQueryValue(String key, String defaultValue) {
    return queries.getOrDefault(key, defaultValue);
  }

  public Optional<Long> getQueryValueAsLong(String key) {
    try {
      return Optional.of(Long.parseLong(queries.get(key)));
    } catch (NumberFormatException ne) {
      return Optional.empty();
    }
  }

  public Optional<Double> getQueryValueAsDouble(String key) {
    try {
      return Optional.of(Double.parseDouble(queries.get(key)));
    } catch (NumberFormatException | NullPointerException ne) {
      return Optional.empty();
    }
  }

  public double getQueryValueAsDouble(String key, double defaultValue) {
    try {
      return Double.parseDouble(queries.get(key));
    } catch (NumberFormatException | NullPointerException ne) {
      return defaultValue;
    }
  }

  public long getQueryValueAsLong(String key, long defaultValue) {
    try {
      return Long.parseLong(queries.get(key));
    } catch (NumberFormatException ne) {
      return defaultValue;
    }
  }

  public int getQueryValueAsInteger(String key, int defaultValue) {
    try {
      return Integer.parseInt(queries.get(key));
    } catch (NumberFormatException ne) {
      return defaultValue;
    }
  }

  public boolean getQueryValueAsBoolean(String key, boolean defaultValue) {
    if (queries.get(key) != null) {
      if (queries.get(key).compareToIgnoreCase("true") == 0) {
        return true;
      }
      if (queries.get(key).compareToIgnoreCase("false") == 0) {
        return false;
      }
    }
    return defaultValue;
  }

  public Optional<Long> getQueryValueAsPositiveLong(String key) {
    long val;
    try {
      val = Long.parseLong(queries.get(key));
      if (val >= 0) {
        return Optional.of(val);
      } else {
        return Optional.empty();
      }
    } catch (NumberFormatException ex) {
      return Optional.empty();
    }
  }

  public Optional<Integer> getQueryValueAsPositiveInteger(String key) {
    int val;
    try {
      val = Integer.parseInt(queries.get(key));
      if (val >= 0) {
        return Optional.of(val);
      } else {
        return Optional.empty();
      }
    } catch (NumberFormatException ex) {
      return Optional.empty();
    }
  }

  public int getQueryValueAsPositiveInteger(String key, int defaultValue) {
    int val;
    try {
      val = Integer.parseInt(queries.get(key));
      if (val >= 0) {
        return val;
      } else {
        return defaultValue;
      }
    } catch (NumberFormatException ex) {
      return defaultValue;
    }
  }

  public long getQueryValueAsPositiveLong(String key, long defaultValue) {
    long val;
    try {
      val = Long.parseLong(queries.get(key));
      if (val >= 0) {
        return val;
      } else {
        return defaultValue;
      }
    } catch (NumberFormatException ex) {
      return defaultValue;
    }
  }

  public long getQueryValueAsTime(String argName, long defaultValue) {
    try {
      return parseTimeToLong(argName);
    } catch (IllegalArgumentException e) {
      return defaultValue;
    }
  }

  public Optional<Long> getQueryValueAsTime(String argName) {
    try {
      return Optional.of(parseTimeToLong(argName));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  private long parseTimeToLong(String query) throws IllegalArgumentException {
    if (!queries.containsKey(query)) {
      throw new IllegalArgumentException("time value is null");
    }
    DateTimeFormatter fmt
            = DateTimeFormat.forPattern(HDSConstants.DEFAULT_TIME_FORMAT);
    DateTime dt = fmt.withZone(
            DateTimeZone.forID(HDSConstants.DEFAULT_TIME_ZONE))
            .parseDateTime(queries.get(query));
    Long time = dt.getMillis();
    return time;
  }

  public int parsePositiveInteger(String argName, int defaultVal) {
    int val;
    try {
      val = Integer.parseInt(queries.get(argName));
      if (val >= 0) {
        return val;
      } else {
        return defaultVal;
      }
    } catch (NumberFormatException ex) {
      return defaultVal;
    }
  }

  public int parsePositiveInteger(String argName,
          int negativeDefaultVal, int defaultVal) {
    int val;
    try {
      val = Integer.parseInt(queries.get(argName));
      if (val >= 0) {
        return val;
      } else {
        return negativeDefaultVal;
      }
    } catch (NumberFormatException ex) {
      return defaultVal;
    }
  }

  public Optional<String> containsAndGet(String argName) {
    if (contains(argName)) {
      return getQueryValue(argName);
    }
    return Optional.empty();
  }

  public boolean contains(String argName) {
    return queries.containsKey(argName);
  }

  public boolean isPresent() {
    return !queries.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Entry<String, String> query : queries.entrySet()) {
      if (first) {
        first = false;
        sb.append(query.getKey()).append("=").append(query.getValue());
      } else {
        sb.append("&").append(query.getKey()).append("=").append(query.getValue());
      }
    }
    return sb.toString();
  }

  private Map<String, String> parseQuery(String rawQuery)
          throws UnsupportedEncodingException, IllegalUriException {
    Map<String, String> query = new TreeMap<>();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return query;
    }
    for (String singleQuery : rawQuery.split("&")) {
      String[] args = singleQuery.split("=");
      if (args.length == 1) {
        query.put(args[0].toLowerCase(), "");
      }
      if (args.length == 2) {
        String uri = URLDecoder.decode(args[1], HDSConstants.DEFAULT_CHAR_ENCODING);
        query.put(args[0].toLowerCase(), uri);

      }
    }
    return query;
  }
}
