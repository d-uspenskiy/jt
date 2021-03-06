package com.duspensky.jutils.rmqrmi;

import java.util.concurrent.Executor;

import com.duspensky.jutils.rmqrmi.Exceptions.BadConfig;

public final class GatewayBuilder {
  private String host_ = "localhost";
  private int port_ = 5672;
  private String vHost_ = "/";
  private Executor executor_;
  private Serializer serializer_;
  private Corellator corellator_;
  private String mainThreadName_;

  public GatewayBuilder setHost(String host) throws BadConfig {
    if (host == null) {
      throw new BadConfig("Host can't be null");
    }

    this.host_ = host;
    return this;
  }

  public GatewayBuilder setPort(int port) {
    this.port_ = port;
    return this;
  }

  public GatewayBuilder setVHost(String vHost) throws BadConfig {
    if (vHost == null) {
      throw new BadConfig("vHost can't be null");
    }

    this.vHost_ = vHost;
    return this;
  }

  public GatewayBuilder setExecutor(Executor executor) throws BadConfig {
    if (executor == null) {
      throw new BadConfig("Executor can't be null");
    }

    this.executor_ = executor;
    return this;
  }

  public GatewayBuilder setSerializer(Serializer serializer) throws BadConfig {
    if (serializer == null) {
      throw new BadConfig("Serializer can't be null");
    }

    this.serializer_ = serializer;
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

  public Gateway build() throws BadConfig {
    if (executor_ == null) {
      throw new BadConfig("Executor can't be null");
    }

    if (serializer_ == null) {
      throw new BadConfig("Serializer can't be null");
    }

    return new GatewayImpl(
        new Config(host_, port_, vHost_, executor_, serializer_, corellator_), mainThreadName_);
  }
}
