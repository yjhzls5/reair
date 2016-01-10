package com.airbnb.di.utils;

import com.mysql.management.MysqldResource;
import com.mysql.management.MysqldResourceI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by paul_yang on 7/30/15.
 */
public class EmbeddedMySqlDb {

    private static final Log LOG = LogFactory.getLog(
            EmbeddedMySqlDb.class);

    private String databaseDir;
    private String databaseName;
    private String host;
    private int port;
    private String username;
    private String password;
    private MysqldResource mysqldResource;

    public EmbeddedMySqlDb() {
        databaseDir = System.getProperty("java.io.tmpdir");
        databaseName = "test_db_" + System.nanoTime();
        databaseName = "test_db_" + System.nanoTime();
        host = "localhost";
        port = new Random().nextInt(10000) + 3306;
        username = "root";
        password = "";
    }

    public void startDb() {
        Map<String, String> databaseOptions = new HashMap<>();
        databaseOptions.put(MysqldResourceI.PORT, Integer.toString(port));

        mysqldResource = new MysqldResource(new File(databaseDir,
                databaseName));
        mysqldResource.start("embedded-mysqld-db-thread-" +
                System.currentTimeMillis(), databaseOptions);

        if (!mysqldResource.isRunning()) {
            throw new RuntimeException("Failed to start embedded MySQL DB!");
        }

        LOG.debug("MySQL started successfully");
    }

    public void stopDb() {
        mysqldResource.shutdown();
        LOG.debug("MySQL stoppped succcessfully");
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}

