package com.duspensky.jutils.common;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Misc {
  public interface FunctionWithException<T, R> {
    R apply(T arg) throws Exception;
  }

  public interface RunnableWithException {
    void run() throws Exception;
  }

  @SuppressWarnings("java:S108")
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

  public static boolean shutdown(ExecutorService service, int awaitMs) throws InterruptedException {
    service.shutdown();
    return service.awaitTermination(awaitMs, TimeUnit.MILLISECONDS);
  }

  public static boolean shutdown(ExecutorService service) throws InterruptedException {
    service.shutdown();
    return service.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  public static void waitAllOperationsProcessed(Executor exec) throws InterruptedException, ExecutionException {
    var completeOperation = new CompletableFuture<Void>();
    exec.execute(() -> completeOperation.complete(null));
    completeOperation.get();
  }

  public static ExecutorService namedThreadExecutor(String threadName) {
    return Executors.newSingleThreadExecutor(r -> new Thread(r, threadName));
  }

  public static <T extends AutoCloseable> CloseableHolder<T> closeableHolder(T closeable) {
    return new CloseableHolder<>(closeable);
  }

  private Misc() {}
}
