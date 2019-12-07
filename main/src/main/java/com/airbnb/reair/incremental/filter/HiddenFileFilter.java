package com.airbnb.reair.incremental.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @Description: hiddenFile filter,
 *            filename not start with .and _
 * @Author: WalterYi
 * @Date: 2019/11/1
 */
public class HiddenFileFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }
}
