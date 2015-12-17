package com.airbnb.di.common;

import org.apache.hadoop.fs.Path;

/**
 * Helps construct Path objects by allowing incremental element additions.
 * For example, /a/b/c can be formed by starting with /a, then adding b and c.
 */
public class PathBuilder {
    private Path currentPath;

    public PathBuilder(Path currentPath) {
        this.currentPath = currentPath;
    }

    public PathBuilder add(String element) {
        currentPath = new Path(currentPath, element);
        return this;
    }

    public Path toPath() {
        return currentPath;
    }
}
