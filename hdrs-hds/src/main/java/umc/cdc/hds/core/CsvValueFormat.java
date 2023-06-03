/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 *
 * @author jpopaholic
 */
public class CsvValueFormat {

  public static String toCsvFormat(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof Float) {
      return BigDecimal.valueOf(((Float) o).doubleValue()).setScale(5, RoundingMode.HALF_UP).toPlainString();
    }
    if (o instanceof Double) {
      return BigDecimal.valueOf((double) o).setScale(5, RoundingMode.HALF_UP).toPlainString();
    }
    if (o instanceof BigDecimal) {
      return ((BigDecimal) o).toPlainString();
    }
    if (o instanceof Date) {
      return new DateTime(((Date) o).getTime()).toString(DateTimeFormat.forPattern(HDSConstants.CSV_TIME_FORMAT));
    }
    if (o instanceof Time) {
      return new DateTime(((Time) o).getTime()).toString(DateTimeFormat.forPattern(HDSConstants.CSV_TIME_FORMAT));
    }
    if (o instanceof Timestamp) {
      return new DateTime(((Timestamp) o).getTime()).toString(DateTimeFormat.forPattern(HDSConstants.CSV_TIME_FORMAT));
    }
    return o.toString();
  }
}
