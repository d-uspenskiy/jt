package com.duspensky.jutils.rmq;

import java.util.concurrent.ExecutorService;

import com.duspensky.jutils.rmi.transport.Processor;

class Config {
  final String host;
  final int port;
  final String vHost;
  final ExecutorService executor;
  final Processor processor;

  public Config(String h, int p, String vH, ExecutorService e, Processor pr) {
    host = h;
    port = p;
    vHost = vH;
    executor = e;
    processor = pr;
  }
}
