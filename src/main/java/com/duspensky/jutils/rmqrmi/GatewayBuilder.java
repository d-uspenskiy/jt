package com.duspensky.jutils.rmqrmi;

import java.util.concurrent.Executor;

import org.apache.commons.lang3.Validate;

public final class GatewayBuilder {
  private String host_ = "localhost";
  private int port_ = 5672;
  private String vHost_ = "/";
  private Executor executor_;
  private Serializer serializer_;
  private Corellator corellator_;
  private String mainThreadName_;

  public GatewayBuilder setHost(String host) {
    this.host_ = Validate.notNull(host);
    return this;
  }

  public GatewayBuilder setPort(int port) {
    this.port_ = port;
    return this;
  }

  public GatewayBuilder setVHost(String vHost) {
    this.vHost_ = Validate.notNull(vHost);
    return this;
  }

  public GatewayBuilder setExecutor(Executor executor) {
    this.executor_ = Validate.notNull(executor);
    return this;
  }

  public GatewayBuilder setSerializer(Serializer serializer) {
    this.serializer_ = Validate.notNull(serializer);
    return this;
  }

  public GatewayBuilder setCorellator(Corellator corellator) {
    this.corellator_ = corellator;
    return this;
  }

  public GatewayBuilder setMainThreadName(String threadName) {
    this.mainThreadName_ = threadName;
    return this;
  }

  public Gateway build() {
    return new GatewayImpl(
        new Config(host_, port_, vHost_, Validate.notNull(executor_), Validate.notNull(serializer_), corellator_),
        mainThreadName_);
  }
}
