package com.duspensky.jutils.rmqrmi;

import com.duspensky.jutils.rmqrmi.Exceptions.BadSerialization;

public interface Serializer {
  byte[] serialize(Object[] objs) throws BadSerialization;
  byte[] serialize(Object obj) throws BadSerialization;

  Object[] deserialize(Class<?>[] cls, byte[] data) throws BadSerialization;
  Object deserialize(Class<?> cl, byte[] data) throws BadSerialization;
}