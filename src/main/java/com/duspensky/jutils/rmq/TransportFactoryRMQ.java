package com.duspensky.jutils.rmq;

import java.util.concurrent.ExecutorService;

import com.duspensky.jutils.rmi.transport.Processor;
import com.duspensky.jutils.rmi.transport.Transport;
import com.duspensky.jutils.rmi.transport.TransportFactory;

import org.apache.commons.lang3.Validate;

public class TransportFactoryRMQ implements TransportFactory {
  private String host = "localhost";
  private int port = 5672;
  private String vHost = "/";

  public void setHost(String h) {
    host = Validate.notNull(h);
  }

  public void setPort(int p) {
    port = p;
  }

  public void setVHost(String vH) {
    vHost = Validate.notNull(vH);
  }

  @Override
  public Transport build(Processor processor, ExecutorService executor) {
    return new TransportImpl(host, port, vHost, Validate.notNull(executor), Validate.notNull(processor));
  }
}
