package com.duspensky.jutils.rmqrmi;

class Config {
  final String host;
  final int port;
  final String vHost;
  final Serializer serializer;
  final Corellator corellator;

  Config(String host, int port, String vHost, Serializer serializer, Corellator correlator) {
    this.host = host;
    this.port = port;
    this.vHost = vHost;
    this.serializer = serializer;
    this.corellator = correlator;
  }
}
