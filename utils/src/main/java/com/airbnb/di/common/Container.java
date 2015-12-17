package com.airbnb.di.common;

/**
 * Container that is used to hold other objects to workaround use cases where an
 * object or null needs to be returned.
 * @param <T>
 */
public class Container<T> {
    private volatile T item;

    public void set(T t) {
        item = t;
    }

    public T get() {
        return item;
    }
}
