package com.duspensky.jutils.rmi;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import com.duspensky.jutils.common.Misc;
import com.duspensky.jutils.common.Misc.RunnableWithException;
import com.duspensky.jutils.rmi.Exceptions.BadInterface;
import com.duspensky.jutils.rmi.Exceptions.BadSerialization;
import com.duspensky.jutils.rmi.ImplementationRegistrator.NameProvider;
import com.duspensky.jutils.rmi.transport.Processor;
import com.duspensky.jutils.rmi.transport.Transport;
import com.duspensky.jutils.rmi.transport.TransportFactory;
import com.duspensky.jutils.rmi.transport.Exceptions.BadTopic;
import com.duspensky.jutils.rmi.transport.Exceptions.BaseException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GatewayImpl implements Gateway, Processor {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayImpl.class);

  private static class NameProviderImpl implements NameProvider {

    @Override
    public String getName(Class<?> iface) {
      return iface.getCanonicalName();
    }

    @Override
    public String getName(Method method) {
      var builder = new StringBuilder();
      builder.append(method.getName());
      builder.append("#");
      var crc32 = new CRC32C();
      crc32.update(method.getReturnType().getCanonicalName().getBytes());
      for (var p : method.getParameterTypes()) {
        crc32.update(p.getCanonicalName().getBytes());
      }
      builder.append(crc32.getValue());
      return builder.toString();
    }
  }

  private final ExecutorService operationExecutor;
  private final NameProvider nameProvider;
  private final ImplementationRegistrator impls;
  private final Transport transport;
  private final Serializer serializer;
  private final Corellator corellator;
  private final long timeoutMs;

  GatewayImpl(TransportFactory factory, Serializer serial, String threadName, Corellator cor, long timeout) {
    serializer = serial;
    corellator = cor;
    timeoutMs = timeout;
    nameProvider = new NameProviderImpl();
    impls = new ImplementationRegistrator(nameProvider);
    operationExecutor = Misc.namedThreadExecutor(ObjectUtils.firstNonNull(threadName, "gateway-main"));
    transport = factory.build(this, operationExecutor);
    run(transport::activate);
  }

  @Override
  public void close() throws InterruptedException, ExecutionException {
    run(transport::deactivate);
    Misc.waitAllOperationsProcessed(operationExecutor);
    Misc.shutdown(operationExecutor);
  }

  @SuppressWarnings("java:S1181")
  @Override
  public <T> void registerImplementation(Class<T> iface, T impl, Executor ex) throws BadInterface {
    checkIfaceIsAcceptable(iface);
    Validate.notNull(impl);
    Validate.notNull(ex);
    var future = new CompletableFuture<Void>();
    run(() -> {
      try {
        registerImpl(iface, impl, ex);
        future.complete(null);
      } catch (Throwable e) {
        future.completeExceptionally(e);
      }
    });
    try {
      getResult(future);
    } catch (Throwable e) {
      throw new BadInterface("Registration failed", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T buildClient(Class<T> iface) throws BadInterface {
    checkIfaceIsAcceptable(iface);
    var methodNames = new HashMap<Method, String>();
    for (var method : iface.getMethods()) {
      methodNames.put(method, nameProvider.getName(method));
    }
    var ifaceName = nameProvider.getName(iface);
    var oneWay = isEventInterface(iface);
    return (T) Proxy.newProxyInstance(
      iface.getClassLoader(), new Class[]{iface},
      (obj, method, args) -> invokeRemote(
          ifaceName, methodNames.get(method), args, oneWay ? null : method.getReturnType()));
  }

  @Override
  public void reconnect() {
    run(() -> {
      transport.deactivate();
      transport.activate();
    });
  }

  @SuppressWarnings("java:S1181")
  private void run(RunnableWithException operation) {
    operationExecutor.execute(() -> {
      try {
        operation.run();
      } catch (Throwable e) {
        LOG.error("Exception", e);
      }
    });
  }

  private void registerImpl(Class<?> iface, Object impl, Executor ex) throws BadTopic, BadInterface {
    impls.register(iface, impl, ex);
    transport.subscribe(nameProvider.getName(iface));
  }

  private Object invokeRemote(
      String ifaceName, String methodName, Object[] args, Class<?> returnType)
      throws BadSerialization, InterruptedException, ExecutionException, TimeoutException {
    LOG.debug("invokeRemote {} on {}", methodName, ifaceName);
    var data = serializer.serialize(args);
    if (returnType != null) {
      var future = new CompletableFuture<byte[]> ();
      run(() -> transport.send(ifaceName, methodName, data, reply -> {
        try {
          future.complete(reply.getData());
        } catch (BaseException e) {
          future.completeExceptionally(e);
        }
      }));
      return serializer.deserialize(returnType, getResult(future));
    }
    run(() -> transport.send(ifaceName, methodName, data, null));
    return null;
  }

  @SuppressWarnings("java:S1181")
  public void onMessage(String ifaceName, String methodName, byte[] data, Consumer<byte[]> reply) {
    LOG.debug("onMessage iface={}, method={}, data_len={}",  ifaceName, methodName, data.length);
    var methodDescr = impls.get(ifaceName, methodName);
    methodDescr.target.executor.execute(() -> {
      try {
        var args = serializer.deserialize(methodDescr.method.getParameterTypes(), data);
        var result = methodDescr.method.invoke(methodDescr.target.obj, args);
        if (reply != null) {
          var response = serializer.serialize(result);
          run(() -> reply.accept(response));
        }
      } catch (Throwable e) {
        LOG.error(String.format("Exception on %s %s", ifaceName, methodName), e);
        if (reply != null) {
          // TODO: send response in case of exception
        }
      }
    });
  }

  private <T> T getResult(Future<T> future) throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  private static void checkIfaceIsAcceptable(Class<?> iface) throws BadInterface {
    if (!iface.isInterface() || !Modifier.isPublic(iface.getModifiers())) {
      throw new BadInterface(String.format("'%s' is not the public accessed interface", iface.getCanonicalName()));
    }
    if (isEventInterface(iface)) {
      for (var method : iface.getMethods()) {
        var rt = method.getReturnType();
        if (!(rt.equals(Void.class) || rt.equals(void.class))) {
          throw new BadInterface(String.format(
              "event interface '%s' has non void method '%s'", iface.getCanonicalName(), method.getName()));
        }
      }
    }
  }

  private static boolean isEventInterface(Class<?> iface) {
    return iface.getAnnotation(EventInterface.class) != null;
  }
}
