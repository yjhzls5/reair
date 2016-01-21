package com.airbnb.di.common;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils {

  private static final Log LOG = LogFactory.getLog(FsUtils.class);

  public static <ReturnType, ExceptionType extends Throwable> ReturnType executeWithRetry(
      Command<ReturnType, ExceptionType> command, int attempts, long baseWait, int exponent,
      int maxWaitTime) {
    throw new RuntimeException("Not yet implemented");
  }

  public static String commonPrefix(String a, String b) {
    for (int i = 0; i < a.length(); i++) {
      // Strings match but we've come to the end of b
      if (i >= b.length()) {
        return b;
      }

      // Strings don't match starting at i
      if (a.charAt(i) != b.charAt(i)) {
        return a.substring(0, i);
      }

      // Strings match, but we've come to he end of a
      if (i == a.length() - 1) {
        return a;
      }
    }
    throw new RuntimeException("Shouldn't get to here!");
  }
}
