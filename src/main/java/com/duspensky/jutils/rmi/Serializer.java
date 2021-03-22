package com.duspensky.jutils.rmi;

import com.duspensky.jutils.rmi.Exceptions.BadSerialization;

public interface Serializer {
  byte[] serialize(Object[] objs) throws BadSerialization;
  byte[] serialize(Object obj) throws BadSerialization;

  Object[] deserialize(Class<?>[] cls, byte[] data) throws BadSerialization;
  Object deserialize(Class<?> cl, byte[] data) throws BadSerialization;
}