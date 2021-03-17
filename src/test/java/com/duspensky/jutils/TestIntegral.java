package com.duspensky.jutils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.lang.reflect.Method;

import com.duspensky.jutils.common.ExecutorHolder;
import com.duspensky.jutils.common.Misc;
import com.duspensky.jutils.rmqrmi.BaseSerializer;
import com.duspensky.jutils.rmqrmi.EventInterface;
import com.duspensky.jutils.rmqrmi.Gateway;
import com.duspensky.jutils.rmqrmi.GatewayBuilder;
import com.duspensky.jutils.rmqrmi.Serializer;
import com.duspensky.jutils.rmqrmi.Exceptions.BadInterface;
import com.duspensky.jutils.rmqrmi.Exceptions.BadSerialization;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIntegral {
  private static final Logger LOG = LoggerFactory.getLogger(TestIntegral.class);

  private static class SimpleStringSerializer extends BaseSerializer {
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

  public interface BasicOperations {
    Integer sum(Integer a, Integer b);
  }

  public interface Operations extends BasicOperations {
    Integer mul(Integer a, Integer b);
    Integer div(Integer a, Integer b);
  }
  @EventInterface
  public interface Notification {
    void onNewResult(Integer value);
  }

  private static class OperationsImpl implements Operations {
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
  }

  private static class NotificationImpl implements Notification {
    private ArrayList<Integer> results_;

    public NotificationImpl(ArrayList<Integer> results) {
      results_ = results;
    }

    @Override
    public void onNewResult(Integer value) {
      results_.add(value);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T callCheckWrapper(Class<T> cl, T obj, Runnable checker) {
    return (T) Proxy.newProxyInstance(
        cl.getClassLoader(), new Class[]{cl}, (Object proxy, Method method, Object[] args) -> {
          checker.run();
          return method.invoke(obj, args);
        });
  }

  private static class Registrator {
    private Runnable checker;
    private Executor executor;

    public Registrator(Runnable c, Executor e) {
      checker = c;
      executor = e;
    }

    public <T> Registrator register(Gateway gw, Class<T> iface, T impl) throws BadInterface {
      gw.registerImplementation(iface, callCheckWrapper(iface, impl, checker), executor);
      return this;
    }
  }

  @Test
  public void basicInvocation() throws Exception {
    final String workerThreadName = "gateway-worker";
    final String gatewayThreadName = "gateway-main";

    Runnable workerThreadChecker = () -> {
      String threadName = Thread.currentThread().getName();
      if (!threadName.equals(workerThreadName)) {
        throw new RuntimeException(String.format(
            "Executed in context of wrong thread '%s', but '%s' is expected", threadName, workerThreadName));
      }
    };
    Runnable nonGatewayTreadChecker = () -> {
      if (Thread.currentThread().getName().equals(gatewayThreadName)) {
        throw new RuntimeException(String.format(
            "Execution in context of '%s' thread is prohibited", gatewayThreadName));
      }
    };

    try (ExecutorHolder ex = new ExecutorHolder(Misc.namedThreadExecutor(workerThreadName))) {
      GatewayBuilder builder = new GatewayBuilder(
          callCheckWrapper(Serializer.class, new SimpleStringSerializer(), nonGatewayTreadChecker))
          .setMainThreadName(gatewayThreadName);
      try (Gateway gw = builder.build();
           Gateway extraGw = builder.build()) {
        ArrayList<Integer> gwResult = new ArrayList<>();
        ArrayList<Integer> extraGwResult = new ArrayList<>();
        new Registrator(workerThreadChecker, ex.get())
            .register(gw, Operations.class, new OperationsImpl())
            .register(gw, Notification.class, new NotificationImpl(gwResult))
            .register(extraGw, Notification.class, new NotificationImpl(extraGwResult));
        Operations opService = gw.buildClient(Operations.class);
        Notification ntfService = gw.buildClient(Notification.class);
        Consumer<Map.Entry<Integer,Integer>> resultForwarder = (val) -> {
          assertEquals(val.getKey(), val.getValue());
          ntfService.onNewResult(val.getKey());
        };
        resultForwarder.accept(Misc.makePair(10, opService.mul(2, 5)));
        resultForwarder.accept(Misc.makePair(5, opService.sum(3, 2)));
        resultForwarder.accept(Misc.makePair(3, opService.div(15, 5)));
        opService.sum(1, 1);
        assertEquals(Arrays.asList(new Integer[]{10, 5, 3}), extraGwResult);
        assertEquals(gwResult, extraGwResult);
      }
    }
  }
}
