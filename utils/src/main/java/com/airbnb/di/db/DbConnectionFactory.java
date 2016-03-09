package com.airbnb.di.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for a factory that returns connections to a DB.
 */
public interface DbConnectionFactory {

  public Connection getConnection() throws SQLException;

}

