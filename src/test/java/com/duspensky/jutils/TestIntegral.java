package com.duspensky.jutils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationTargetException;

import com.duspensky.jutils.common.ThreadExecutor;
import com.duspensky.jutils.rmqrmi.Gateway;
import com.duspensky.jutils.rmqrmi.GatewayBuilder;
import com.duspensky.jutils.rmqrmi.Serializer;
import com.duspensky.jutils.rmqrmi.Exceptions.BadSerialization;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIntegral {
  private static final Logger LOG = LoggerFactory.getLogger(TestIntegral.class);
  private static final String GATEWAY_MAIN_THREAD = "gateway-main";

  public interface IBasicOperations {
    Integer sum(Integer a, Integer b);
  }

  public interface IOperations extends IBasicOperations {
    Integer mul(Integer a, Integer b);
    Integer div(Integer a, Integer b);
  }

  private static class SimpleStringSerializer extends Serializer {
    @Override
    public byte[] serialize(Object[] objs) {
      checkThread();
      StringBuilder builder = new StringBuilder();
      for (Object o : objs) {
        builder.append(o.toString());
        builder.append("#");
      }
      final int l = builder.length();
      if (l > 0) {
        builder.setLength(l - 1);
      }
      return builder.toString().getBytes();
    }

    @Override
    public Object[] deserialize(Class<?>[] cls, byte[] data) throws BadSerialization {
      checkThread();
      String val = new String(data);
      String[] items = val.split("#");
      if (items.length != cls.length) {
        throw new BadSerialization(
            String.format("Wrong number of argument %d expected, but %d found", cls.length, items.length));
      }
      Object[] result = new Object[cls.length];
      for (int i = 0; i < cls.length; ++i) {
        try {
          result[i] = cls[i].getConstructor(String.class).newInstance(items[i]);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | NoSuchMethodException | SecurityException e) {
          throw new BadSerialization(String.format("Failed to create instance of %s", cls[i].getCanonicalName()), e);
        }
      }
      return result;
    }

    private void checkThread() {
      if (Thread.currentThread().getName().equals(GATEWAY_MAIN_THREAD)) {
        throw new RuntimeException("Serialization is called in context of gateway thread");
      }
    }
  }

  private static class Operations implements IOperations {
    private String expectedThreadPrefix_;

    public Operations(String expectedThreadPrefix) {
      expectedThreadPrefix_ = expectedThreadPrefix;
    }

    @Override
    public Integer sum(Integer a, Integer b) {
      checkThread();
      return a + b;
    }

    @Override
    public Integer mul(Integer a, Integer b) {
      checkThread();
      return a * b;
    }

    @Override
    public Integer div(Integer a, Integer b) {
      checkThread();
      return a / b;
    }

    private void checkThread() {
      String threadName = Thread.currentThread().getName();
      if (!threadName.startsWith(expectedThreadPrefix_)) {
        throw new RuntimeException(String.format(
            "Executed in context of wrong thread '%s', but '%s' prefix is expected",
            threadName, expectedThreadPrefix_));
      }
    }
  }

  @Test
  public void basicInvocation() throws Exception {
    final String workerThreadName = "gateway-worker";
    try (ThreadExecutor ex = new ThreadExecutor(workerThreadName);
         Gateway gw = new GatewayBuilder()
                        .setExecutor(ex)
                        .setSerializer(new SimpleStringSerializer())
                        .setMainThreadName(GATEWAY_MAIN_THREAD)
                        .build()) {
      gw.register(IOperations.class, new Operations(workerThreadName));
      IOperations service = gw.stub(IOperations.class);
      assertEquals(10, service.mul(2, 5));
      assertEquals(5, service.sum(3, 2));
      assertEquals(3, service.div(15, 5));
    }
  }
}
