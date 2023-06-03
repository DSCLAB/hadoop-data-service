/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.uri;

import java.util.Map;
import java.util.Optional;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.Protocol;
import umc.cdc.hds.parser.RangeValue;

/**
 *
 * @author brandboat
 */
public class DataStorageQuery extends Query {

  private final Optional<Protocol> location;
  private final boolean keep;
  private final boolean clone;
  private final boolean directory;
  private final Optional<String> name;
  private final Optional<String> key;
  private final Optional<Long> time;
  private final Optional<Long> size;
  private final Optional<RangeValue<String, String>> keyRange;
  private final Optional<RangeValue<Long, Long>> timeRange;
  private final Optional<RangeValue<Long, Long>> sizeRange;

  public DataStorageQuery(Map<String, String> query) {
    super(query);
    location = Protocol.find(
        getQueryValue(HDSConstants.HDS_URI_ARG_LOCATION).orElse(null));
    keep = getQueryValueAsBoolean(HDSConstants.COMMON_URI_ARG_KEEP, true);
    clone = getQueryValueAsBoolean(HDSConstants.HDS_URI_ARG_CLONE, true);
    directory = getQueryValueAsBoolean(HDSConstants.DS_URI_ARG_DIRECTORY, false);
    name = getQueryValue(HDSConstants.HDS_URI_ARG_NAME);
    key = getQueryValue(HDSConstants.HDS_URI_ARG_KEY);
    time = getQueryValueAsTime(HDSConstants.HDS_URI_ARG_TIME);
    size = getQueryValueAsLong(HDSConstants.HDS_URI_ARG_SIZE);
    keyRange = createKeyRange();
    timeRange = createTimeRange();
    sizeRange = createSizeRange();
  }

  public Optional<Protocol> getLocation() {
    return location;
  }

  public boolean getKeep() {
    return keep;
  }

  public boolean getClone() {
    return clone;
  }
  
  public boolean getDirectory() {
    return directory;
  }

  public Optional<String> getName() {
    return name;
  }

  public Optional<String> getKey() {
    return key;
  }

  public Optional<Long> getTime() {
    return time;
  }

  public Optional<Long> getSize() {
    return size;
  }

  public Optional<RangeValue<String, String>> getKeyRange() {
    return keyRange;
  }

  public Optional<RangeValue<Long, Long>> getTimeRange() {
    return timeRange;
  }

  public Optional<RangeValue<Long, Long>> getSizeRange() {
    return sizeRange;
  }

  private Optional<RangeValue<String, String>> createKeyRange() {
    Optional<String> minValue = getQueryValue(HDSConstants.HDS_URI_ARG_MINKEY),
        maxValue = getQueryValue(HDSConstants.HDS_URI_ARG_MAXKEY);
    if (minValue.isPresent() && maxValue.isPresent()) {
      return Optional.of(new RangeValue(minValue.get(), maxValue.get()));
    }
    return Optional.empty();
  }

  private Optional<RangeValue<Long, Long>> createTimeRange() {
    long minValue = getQueryValueAsTime(HDSConstants.HDS_URI_ARG_MINTIME, 0),
        maxValue = getQueryValueAsTime(HDSConstants.HDS_URI_ARG_MAXTIME, Long.MAX_VALUE);
    return Optional.of(new RangeValue(minValue, maxValue));
  }

  private Optional<RangeValue<Long, Long>> createSizeRange() {
    long minValue = getQueryValueAsLong(HDSConstants.HDS_URI_ARG_MINSIZE, 0),
        maxValue = getQueryValueAsLong(HDSConstants.HDS_URI_ARG_MAXSIZE, Long.MAX_VALUE);
    return Optional.of(new RangeValue(minValue, maxValue));
  }
}
