package com.duspensky.jutils.rmqrmi;

import java.util.concurrent.Executor;
import static com.duspensky.jutils.rmqrmi.Exceptions.BadConfig;

public class Config {
  public final String host;
  public final int port;
  public final String vHost;
  public final Executor executor;
  public final Serializer serializer;
  public final Corellator corellator;

  private Config(String host, int port, String vHost, Executor executor, Serializer serializer, Corellator correlator) {
    this.host = host;
    this.port = port;
    this.vHost = vHost;
    this.executor = executor;
    this.serializer = serializer;
    this.corellator = correlator;
  }

  public static class Builder {
    private String host_ = "localhost";
    private int port_ = 5672;
    private String vHost_ = "/";
    private Executor executor_;
    private Serializer serializer_;
    private Corellator corellator_;

    public Builder setHost(String host) throws BadConfig {
      if (host == null) {
        throw new BadConfig("Host can't be null");
      }

      this.host_ = host;
      return this;
    }

    public Builder setPort(int port) {
      this.port_ = port;
      return this;
    }

    public Builder setVHost(String vHost) throws BadConfig {
      if (vHost == null) {
        throw new BadConfig("vHost can't be null");
      }

      this.vHost_ = vHost;
      return this;
    }

    public Builder setExecutor(Executor executor) throws BadConfig {
      if (executor == null) {
        throw new BadConfig("Executor can't be null");
      }

      this.executor_ = executor;
      return this;
    }

    public Builder setSerializer(Serializer serializer) throws BadConfig {
      if (serializer == null) {
        throw new BadConfig("Serializer can't be null");
      }

      this.serializer_ = serializer;
      return this;
    }

    public Builder setCorellator(Corellator corellator) {
      this.corellator_ = corellator;
      return this;
    }

    public Config build() throws BadConfig {
      if (executor_ == null) {
        throw new BadConfig("Executor can't be null");
      }

      if (serializer_ == null) {
        throw new BadConfig("Serializer can't be null");
      }
      return new Config(host_, port_, vHost_, executor_, serializer_, corellator_);
    }
  }
}
