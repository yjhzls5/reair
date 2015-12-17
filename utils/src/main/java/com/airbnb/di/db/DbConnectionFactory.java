package com.airbnb.di.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface DbConnectionFactory {

    public Connection getConnection() throws SQLException ;

}

