package com.duspensky.jutils.common;

import java.util.concurrent.ExecutorService;

public class ExecutorHolder implements AutoCloseable {
  private ExecutorService service;

  public ExecutorHolder(ExecutorService srv) {
    service = srv;
  }

  public ExecutorService get() {
    return service;
  }

  @Override
  public void close() throws Exception {
    while(!Misc.shutdown(service));
  }
}
