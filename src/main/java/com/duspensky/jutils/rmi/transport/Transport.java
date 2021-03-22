package com.duspensky.jutils.rmi.transport;

import java.util.function.Consumer;

import com.duspensky.jutils.rmi.transport.Exceptions.BadTopic;
import com.duspensky.jutils.rmi.transport.Exceptions.BrokenConnection;

public interface Transport {
  void subscribe(String topic) throws BadTopic;
  void unsubscribe(String topic) throws BadTopic;
  void send(String topic, String subTopic, byte[] data, Consumer<Reply> consumer) throws BrokenConnection;
  void activate() throws BrokenConnection;
  void deactivate();
}
