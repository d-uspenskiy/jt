package com.duspensky.jutils.common;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecutorAsService implements ExecutorService {
  private Executor impl_;

  public ExecutorAsService(Executor exec) {
    impl_ = exec;
  }

  @Override
  public void execute(Runnable cmd) {
    impl_.execute(cmd);
  }

  @Override
  public boolean awaitTermination(long arg0, TimeUnit arg1) throws InterruptedException {
    notSupported();
    return false;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0) throws InterruptedException {
    notSupported();
    return Collections.emptyList();
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0, long arg1, TimeUnit arg2)
      throws InterruptedException {
    notSupported();
    return Collections.emptyList();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> arg0) throws InterruptedException, ExecutionException {
    notSupported();
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> arg0, long arg1, TimeUnit arg2)
      throws InterruptedException, ExecutionException, TimeoutException {
    notSupported();
    return null;
  }

  @Override
  public boolean isShutdown() {
    return false;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public void shutdown() {
    notSupported();
  }

  @Override
  public List<Runnable> shutdownNow() {
    notSupported();
    return Collections.emptyList();
  }

  @Override
  public <T> Future<T> submit(Callable<T> arg0) {
    notSupported();
    return null;
  }

  @Override
  public Future<?> submit(Runnable arg0) {
    notSupported();
    return null;
  }

  @Override
  public <T> Future<T> submit(Runnable arg0, T arg1) {
    notSupported();
    return null;
  }

  private void notSupported() {
    throw new UnsupportedOperationException("Method is not supported");
  }
}
