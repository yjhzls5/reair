package com.airbnb.di.hive.replication.webserver;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateUtils {
  /**
   * TODO.
   *
   * @param time TODO
   * @return TODO
   */
  public String convertToIso8601(long time) {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    df.setTimeZone(tz);
    return df.format(time);
  }
}
