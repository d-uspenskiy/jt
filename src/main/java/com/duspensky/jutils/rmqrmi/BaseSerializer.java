package com.duspensky.jutils.rmqrmi;

import com.duspensky.jutils.rmqrmi.Exceptions.BadSerialization;

public abstract class BaseSerializer implements Serializer {
  public byte[] serialize(Object obj) throws BadSerialization {
    return serialize(new Object[]{obj});
  }

  public Object deserialize(Class<?> cl, byte[] data) throws BadSerialization {
    return deserialize(new Class<?>[]{cl}, data)[0];
  }
}
