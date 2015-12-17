package com.airbnb.di.hive.hooks;

import com.airbnb.di.db.DbCredentials;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Class for getting DB credentials from deployed files that contain data in
 * plain text. Caches read values with a expiration time so that we don't
 * re-read files repeatedly.
 */
public class DataInfraDbCredentials implements DbCredentials {
    private static String RW_USER_FILE =
            "/etc/service/mesos_slave/env/DATA_INFRA_RW_USER";
    private static String RW_USER_PWD_FILE =
            "/etc/service/mesos_slave/env/DATA_INFRA_RW_USER_PWD";
    // How often to re-read credentials from files (in case they get rotated)
    private static long EXPIRATION = 10 * 60 * 1000;

    private String rwUser;
    private String rwPwd;
    // Last time the creds were read in unix time ms
    private long lastReadTime;

    @Override
    public void refreshCredsIfNecessary() throws IOException {
        if (rwUser == null || rwPwd == null ||
                System.currentTimeMillis() - lastReadTime > EXPIRATION) {
            lastReadTime = System.currentTimeMillis();
            rwUser = readFile(RW_USER_FILE);
            rwPwd = readFile(RW_USER_PWD_FILE);
        }
    }

    @Override
    public String getReadWriteUsername() throws IOException {
        refreshCredsIfNecessary();
        return rwUser;
    }

    @Override
    public String getReadWritePassword() throws IOException {
        refreshCredsIfNecessary();
        return rwPwd;
    }

    private static String readFile(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        return new String(bytes);
    }
}
