package com.duspensky.jutils.rmi;

import com.duspensky.jutils.rmi.Exceptions.BadSerialization;

public abstract class BaseSerializer implements Serializer {
  public byte[] serialize(Object obj) throws BadSerialization {
    return serialize(new Object[]{obj});
  }

  public Object deserialize(Class<?> cl, byte[] data) throws BadSerialization {
    return deserialize(new Class<?>[]{cl}, data)[0];
  }
}
