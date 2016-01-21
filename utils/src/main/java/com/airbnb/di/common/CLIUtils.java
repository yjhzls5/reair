package com.airbnb.di.common;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class CLIUtils {
  public static void printHelp(String command, Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(command, options);
  }
}
