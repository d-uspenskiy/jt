package com.duspensky.jutils.rmqrmi;

import static com.duspensky.jutils.rmqrmi.Exceptions.BadInterface;

public interface IGateway {
  <T> void register(Class<T> cl, T impl) throws BadInterface;
  <T> T stub(Class<T> cl) throws BadInterface;
  void reconnect();
}