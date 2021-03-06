package com.duspensky.jutils.rmqrmi;

import com.duspensky.jutils.rmqrmi.Exceptions.BadInterface;

public interface Gateway extends AutoCloseable {
  <T> void registerImplementation(Class<T> cl, T impl) throws BadInterface;
  <T> T buildClient(Class<T> cl) throws BadInterface;

  void reconnect();
}