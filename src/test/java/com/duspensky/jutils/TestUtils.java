package com.duspensky.jutils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.duspensky.jutils.common.ExecutorHolder;
import com.duspensky.jutils.rmi.BaseSerializer;
import com.duspensky.jutils.rmi.Gateway;
import com.duspensky.jutils.rmi.GatewayFactory;
import com.duspensky.jutils.rmi.Serializer;
import com.duspensky.jutils.rmi.Exceptions.BadInterface;
import com.duspensky.jutils.rmi.Exceptions.BadSerialization;
import com.duspensky.jutils.rmi.transport.TransportFactory;

import org.apache.commons.lang3.Validate;

class TestUtils {
  private static final String GATEWAY_MAIN_THREAD_NAME = "gateway-main";
  private static final String GATEWAY_WORKER_THREAD_NAME = "gateway-warker";

  static class SimpleStringSerializer extends BaseSerializer {
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
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException 
            | InvocationTargetException | NoSuchMethodException | SecurityException e) {
          throw new BadSerialization(String.format("Failed to create instance of %s", cls[i].getCanonicalName()), e);
        }
      }
      return result;
    }
  }

  static class Registrator implements AutoCloseable {
    private final ExecutorHolder executorHolder;
    private final Gateway gateway;
    private final ExecutorService executor;
    private final Runnable checker;

    Registrator(Gateway g, ExecutorService e, Runnable c) {
      gateway = g;
      checker = c;
      if (e == null) {
        executor = Executors.newSingleThreadExecutor();
        executorHolder = new ExecutorHolder(executor);
      } else {
        executor = e;
        executorHolder = null;
      }
    }

     <T> void register(Class<T> cl, T impl) throws BadInterface {
      gateway.registerImplementation(cl, checker == null ? impl : callCheckWrapper(cl, impl, checker), executor);
    }

    @Override
    public void close() throws Exception {
      if (executorHolder != null) {
        executorHolder.close();
      }
    }
  }

  static class GatewayHelper implements AutoCloseable {
    final Gateway gateway;
    final Registrator registrator;

    GatewayHelper(
        TransportFactory factory, Serializer serializer, long timeoutMS, 
        String mainThreadName, ExecutorService executor, Runnable checker) {
      gateway = new GatewayFactory().build(factory, serializer, null, mainThreadName, timeoutMS);
      registrator = new Registrator(gateway, executor, checker);
    }

    @Override
    public void close() throws Exception {
      registrator.close();
      gateway.close();
    }
  }

  static class GatewayHelperBuilder {
    private TransportFactory transportFactory;
    private Serializer serializer;
    private long timeoutMs = 3000;
    private String mainThreadName;
    private ExecutorService executor;
    private Runnable checker;

    GatewayHelperBuilder setTransportFactory(TransportFactory tF) {
      transportFactory = Validate.notNull(tF);
      return this;
    }

    GatewayHelperBuilder setSerializer(Serializer s) {
      serializer = Validate.notNull(s);
      return this;
    }

    GatewayHelperBuilder setTimeoutMS(long timeout) {
      timeoutMs = timeout;
      return this;
    }

    GatewayHelperBuilder setMainThreadName(String tN) {
      mainThreadName = tN;
      return this;
    }

    GatewayHelperBuilder setExecutor(ExecutorService e) {
      executor = e;
      return this;
    }

    GatewayHelperBuilder setChecker(Runnable c) {
      checker = c;
      return this;
    }

    public GatewayHelper build() {
      return new GatewayHelper(transportFactory, serializer, timeoutMs, mainThreadName, executor, checker);
    }
  }

  @SuppressWarnings("unchecked")
  static <T> T callCheckWrapper(Class<T> cl, T obj, Runnable checker) {
    return (T) Proxy.newProxyInstance(
        cl.getClassLoader(), new Class[]{cl}, (proxy, method, args) -> {
          checker.run();
          return method.invoke(obj, args);
        });
  }

  static GatewayHelper gatewayHelper(TransportFactory factory, ExecutorService executor, Runnable checker) {
    Runnable nonGatewayTreadChecker = () -> {
      if (Thread.currentThread().getName().equals(GATEWAY_MAIN_THREAD_NAME)) {
        throw new RuntimeException(String.format(
            "Execution in context of '%s' thread is prohibited", GATEWAY_MAIN_THREAD_NAME));
      }
    };

    return new GatewayHelperBuilder()
        .setTransportFactory(factory)
        .setSerializer(callCheckWrapper(
            Serializer.class, new SimpleStringSerializer(), nonGatewayTreadChecker))
        .setExecutor(executor)
        .setMainThreadName(GATEWAY_MAIN_THREAD_NAME)
        .setChecker(checker)
        .build();
  }

  static ExecutorService buildWorkerExecutor() {
    return Executors.newSingleThreadExecutor(r -> new Thread(r, GATEWAY_WORKER_THREAD_NAME));
  }

  static Runnable workerThreadChecker() {
    return () -> {
      String threadName = Thread.currentThread().getName();
      if (!threadName.equals(GATEWAY_WORKER_THREAD_NAME)) {
        throw new RuntimeException(String.format(
            "Executed in context of wrong thread '%s', but '%s' is expected", threadName, GATEWAY_WORKER_THREAD_NAME));
      }
    };
  }
  private TestUtils() {}
}
