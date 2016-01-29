package com.airbnb.di.hive.batchreplication.template;

/**
 * Exception thrown when there is an error rendering a template using Velocity.
 */
public class TemplateRenderException extends Exception {
  public TemplateRenderException(String message) {
    super(message);
  }

  public TemplateRenderException(String message, Throwable cause) {
    super(message, cause);
  }
}
