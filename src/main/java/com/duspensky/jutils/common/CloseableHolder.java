package com.duspensky.jutils.common;

public class CloseableHolder<T extends AutoCloseable> implements AutoCloseable {
  private T closeable;

  public CloseableHolder(T c) {
    closeable = c;
  }

  public T get() {
    return closeable;
  }

  public T release() {
    var result = closeable;
    closeable = null;
    return result;
  }

  @Override
  public void close() throws Exception {
    if (closeable != null) {
      closeable.close();
    }
  }
}
