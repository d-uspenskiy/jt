package com.duspensky.jutils.rmi;

import com.duspensky.jutils.rmi.transport.TransportFactory;

import org.apache.commons.lang3.Validate;

public final class GatewayFactory {
  public Gateway build(
      TransportFactory transport, Serializer serializer, Corellator corellator, String mainThreadName, long timeoutMs) {
    return new GatewayImpl(
        Validate.notNull(transport), Validate.notNull(serializer), mainThreadName, corellator, timeoutMs);
  }
}
