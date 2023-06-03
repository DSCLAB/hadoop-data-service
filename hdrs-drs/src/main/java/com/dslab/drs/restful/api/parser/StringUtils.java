
package com.dslab.drs.restful.api.parser;


public final class StringUtils {

  public static boolean equals(String lhs, String rhs) {
    return equals(lhs, rhs, false);
  }

  public static boolean equals(String lhs, String rhs, boolean ignoreCase) {
    if (lhs == null) {
      return rhs == null;
    }
    return ignoreCase ? lhs.equalsIgnoreCase(rhs) : lhs.equals(rhs);
  }

  /**
   * private ctor for util class.
   */
  private StringUtils() {
  }
  
}
