package com.duspensky.jutils.rmqrmi;

public interface Corellator {
  String getCorrelationId();
  void setCorellationId(String correlationId);
}
