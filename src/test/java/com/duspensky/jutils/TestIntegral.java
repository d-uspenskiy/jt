package com.duspensky.jutils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationTargetException;

import com.duspensky.jutils.common.ThreadExecutor;
import com.duspensky.jutils.rmqrmi.Config;
import com.duspensky.jutils.rmqrmi.Gateway;
import com.duspensky.jutils.rmqrmi.Serializer;
import com.duspensky.jutils.rmqrmi.Exceptions.BadSerialization;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIntegral {
  private static final Logger LOG = LoggerFactory.getLogger(TestIntegral.class);

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
  }

  @Test
  public void basicInvocation() throws Exception {
    try (ThreadExecutor ex = new ThreadExecutor();
         Gateway gw = new Gateway(new Config.Builder()
                                            .setExecutor(ex)
                                            .setSerializer(new SimpleStringSerializer())
                                            .build())) {
      gw.register(IOperations.class, new IOperations() {
        @Override
        public Integer sum(Integer a, Integer b) {
          return a + b;
        }

        @Override
        public Integer mul(Integer a, Integer b) {
          return a * b;
        }

        @Override
        public Integer div(Integer a, Integer b) {
          return a / b;
        }
      });
      IOperations service = gw.stub(IOperations.class);
      assertEquals(10, service.mul(2, 5));
      assertEquals(5, service.sum(3, 2));
      assertEquals(3, service.div(15, 5));
    }
  }
}
