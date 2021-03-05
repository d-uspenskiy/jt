package com.duspensky.jutils.rmqrmi;

import static  com.duspensky.jutils.rmqrmi.Exceptions.BadSerialization;

public abstract class Serializer {
  public abstract byte[] serialize(Object[] objs) throws BadSerialization;

  public abstract Object[] deserialize(Class<?>[] cls, byte[] data) throws BadSerialization;

  public byte[] serialize(Object obj) throws BadSerialization {
    return serialize(new Object[]{obj});
  }

  public Object deserialize(Class<?> cl, byte[] data) throws BadSerialization {
    return deserialize(new Class<?>[]{cl}, data)[0];
  }
}