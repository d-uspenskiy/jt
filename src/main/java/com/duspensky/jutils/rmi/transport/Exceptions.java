package com.duspensky.jutils.rmi.transport;

public class Exceptions {
  public abstract static class BaseException extends Exception {
    private static final long serialVersionUID = 8515857559644840884L;

    BaseException(String message) { super(message); }

    BaseException(String message, Throwable cause) { super(message, cause); }

    BaseException(Throwable cause) { super(cause); }
  }

  public static class BrokenConnection extends BaseException {
    private static final long serialVersionUID = -1580614636817533258L;

    public BrokenConnection(String message) { super(message); }

    public BrokenConnection(String message, Throwable cause) { super(message, cause); }
  }

  public static class BadTopic extends BaseException {
    private static final long serialVersionUID = -2373371001306115943L;

    public BadTopic(String message) { super(message); }
  }

  private Exceptions() {}
}
