package com.duspensky.jutils.rmqrmi;

public interface Exceptions {

  public abstract static class Common extends Exception {
    private static final long serialVersionUID = 1179164467268044851L;

    Common(String message) { super(message); }

    Common(String message, Throwable cause) { super(message, cause); }

    Common(Throwable cause) { super(cause); }
  }

  public static class BadConfig extends Common {
    private static final long serialVersionUID = -8063865182726125902L;

    public BadConfig(String message) { super(message); }
  }

  public static class BadInterface extends Common {
    private static final long serialVersionUID = 6584605744637459610L;

    public BadInterface(String message) { super(message); }
  }

  public static class BadInvocation extends Common {
    private static final long serialVersionUID = -5758584321857454692L;

    public BadInvocation(String message) { super(message); }

    public BadInvocation(String message, Throwable cause) { super(message, cause); }
  }

  public static class BadSerialization extends Common {
    private static final long serialVersionUID = -8840798331695828500L;

    public BadSerialization(String message) { super(message); }

    public BadSerialization(String message, Throwable cause) { super(message, cause); }
  }
}
