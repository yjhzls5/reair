package com.airbnb.di.utils;

import com.airbnb.di.db.DbCredentials;

import java.io.IOException;

/**
 * Credentials for connecting to the EmbeddedMySqlDb.
 */
public class TestDbCredentials implements DbCredentials {
  @Override
  public void refreshCredsIfNecessary() throws IOException {
  }

  @Override
  public String getReadWriteUsername() throws IOException {
    return "root";
  }

  @Override
  public String getReadWritePassword() throws IOException {
    return "";
  }
}
