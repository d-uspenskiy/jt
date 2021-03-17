package com.duspensky.jutils.rmqrmi;

import java.util.concurrent.Executor;

import com.duspensky.jutils.rmqrmi.Exceptions.BadInterface;

public interface Gateway extends AutoCloseable {
  <T> void registerImplementation(Class<T> cl, T impl, Executor executor) throws BadInterface;
  <T> T buildClient(Class<T> cl) throws BadInterface;

  void reconnect();
}