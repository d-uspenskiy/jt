package com.duspensky.jutils.rmi.transport;

import java.util.function.Consumer;

public interface Processor {
  void onMessage(String topic, String subTopic, byte[] data, Consumer<byte[]> reply);
}
