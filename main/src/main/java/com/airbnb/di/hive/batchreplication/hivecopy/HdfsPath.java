package com.airbnb.di.hive.batchreplication.hivecopy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * helper class to handle hdfs path
 */
public class HdfsPath {
    private static final String FILE_LOCATION_PATTERN = "(([\\w]+)://([^/]*))?(/.*)";
    private static Pattern locationPattern = Pattern.compile(FILE_LOCATION_PATTERN);

    private final String proto;
    private final String host;
    private final String path;

    public HdfsPath(String fullPath) {
        Matcher matcher = locationPattern.matcher(fullPath.trim());

        if (matcher.matches() && matcher.groupCount() != 5) {
            this.proto = matcher.group(2);
            this.host = matcher.group(3);
            this.path = matcher.group(4);
        } else {

            throw new RuntimeException("Invalide path: " + fullPath + ", matchers? " + matcher.matches() + ", matchGroupCount=" + matcher.groupCount() );
        }
    }

    public String replaceHost(String newHost) {
        return String.format("%s://%s%s", proto, newHost, path);
    }

    public String getPath() {
        return path;
    }

    public String getHost() {
        return host;
    }

    public String getProto() {
        return proto;
    }

    public String getFullPath() {
        return String.format("%s://%s%s", proto, host, path);
    }

}
