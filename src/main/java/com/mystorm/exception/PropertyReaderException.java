package com.mystorm.exception;

public class PropertyReaderException extends RuntimeException {

  public PropertyReaderException(String msg) {
    super(msg);
  }

  public PropertyReaderException(String msg, Throwable ex) {
    super(msg, ex);
  }
}
