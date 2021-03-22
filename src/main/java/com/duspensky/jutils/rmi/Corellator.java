package com.duspensky.jutils.rmi;

public interface Corellator {
  String getCorrelationId();
  void setCorellationId(String correlationId);
}
