package com.duspensky.jutils.common;

import java.util.AbstractMap;
import java.util.Map;

public class Misc {
  private Misc() {}

  public interface FunctionWithException<T, R> {
    R apply(T arg) throws Exception;
  }

  public interface RunnableWithException {
    void run() throws Exception;
  }

  public static void silentClose(AutoCloseable closable) {
    if (closable != null) {
      try {
        closable.close();
      } catch (Exception e) {
      }
    }
  }

  public static <K, V> Map.Entry<K,V> makePair(K k, V v) {
    return new AbstractMap.SimpleImmutableEntry<>(k, v);
  }
}
