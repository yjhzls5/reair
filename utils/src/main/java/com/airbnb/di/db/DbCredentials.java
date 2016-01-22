package com.airbnb.di.db;

import java.io.IOException;

public interface DbCredentials {

  /**
   * Called if the credentials should be refreshed (e.g. re-read from file).
   *
   * @throws IOException TODO
   */
  public void refreshCredsIfNecessary() throws IOException;

  /**
   * TODO.
   *
   * @return the username that has read / write access to the DB
   *
   * @throws IOException TODO
   */
  public String getReadWriteUsername() throws IOException;

  /**
   * TODO.
   *
   * @return the password for the user that has read / write access to the DB
   *
   * @throws IOException TODO
   */
  public String getReadWritePassword() throws IOException;
}
