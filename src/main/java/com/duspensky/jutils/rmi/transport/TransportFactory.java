package com.duspensky.jutils.rmi.transport;

import java.util.concurrent.ExecutorService;

public interface TransportFactory {
  Transport build(Processor processor, ExecutorService executor);
}
