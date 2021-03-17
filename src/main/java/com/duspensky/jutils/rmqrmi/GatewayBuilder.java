package com.duspensky.jutils.rmqrmi;

import org.apache.commons.lang3.Validate;

public final class GatewayBuilder {
  private String host = "localhost";
  private int port = 5672;
  private String vHost = "/";
  private Serializer serializer;
  private Corellator corellator;
  private String mainThreadName;

  public GatewayBuilder(Serializer s) {
    serializer = Validate.notNull(s);
  }

  public GatewayBuilder setHost(String h) {
    host = Validate.notNull(h);
    return this;
  }

  public GatewayBuilder setPort(int p) {
    port = p;
    return this;
  }

  public GatewayBuilder setVHost(String vH) {
    vHost = Validate.notNull(vH);
    return this;
  }

  public GatewayBuilder setCorellator(Corellator c) {
    corellator = c;
    return this;
  }

  public GatewayBuilder setMainThreadName(String threadName) {
    this.mainThreadName = threadName;
    return this;
  }

  public Gateway build() {
    return new GatewayImpl(new Config(host, port, vHost, serializer, corellator), mainThreadName);
  }
}
