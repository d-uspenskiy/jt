package com.duspensky.jutils.rmi.transport;

import com.duspensky.jutils.rmi.transport.Exceptions.BaseException;

public class Reply {
  private final byte[] data;
  private final BaseException exception;

  public Reply(byte[] d, BaseException e) {
    data = d;
    exception = e;
  }

  public byte[] getData() throws BaseException {
    if (exception != null)
      throw exception;
    return data;
  }
}
