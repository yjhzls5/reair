package com.airbnb.di.common;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils {

  private static final Log LOG = LogFactory.getLog(FsUtils.class);

  public static <ReturnT, ExceptionT extends Throwable> ReturnT executeWithRetry(
      Command<ReturnT, ExceptionT> command,
      int attempts,
      long baseWait,
      int exponent,
      int maxWaitTime) {
    throw new RuntimeException("Not yet implemented");
  }

  /**
   * TODO.
   *
   * @param str1 TODO
   * @param str2 TODO
   * @return TODO
   */
  public static String commonPrefix(String str1, String str2) {
    for (int i = 0; i < str1.length(); i++) {
      // Strings match but we've come to the end of str2
      if (i >= str2.length()) {
        return str2;
      }

      // Strings don't match starting at i
      if (str1.charAt(i) != str2.charAt(i)) {
        return str1.substring(0, i);
      }

      // Strings match, but we've come to he end of str1
      if (i == str1.length() - 1) {
        return str1;
      }
    }
    throw new RuntimeException("Shouldn't get to here!");
  }
}
