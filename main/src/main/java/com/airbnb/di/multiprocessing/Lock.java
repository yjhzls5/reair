package com.airbnb.di.multiprocessing;

public class Lock {

  public enum Type {
    SHARED, EXCLUSIVE
  }

  private Type type;
  private String name;

  public Lock(Type type, String name) {
    this.type = type;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("<Lock type: %s name: %s>", type, name);
  }
}
