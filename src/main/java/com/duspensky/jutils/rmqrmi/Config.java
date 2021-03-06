package com.duspensky.jutils.rmqrmi;

import java.util.concurrent.Executor;

class Config {
  final String host;
  final int port;
  final String vHost;
  final Executor executor;
  final Serializer serializer;
  final Corellator corellator;

  Config(String host, int port, String vHost, Executor executor, Serializer serializer, Corellator correlator) {
    this.host = host;
    this.port = port;
    this.vHost = vHost;
    this.executor = executor;
    this.serializer = serializer;
    this.corellator = correlator;
  }
}
