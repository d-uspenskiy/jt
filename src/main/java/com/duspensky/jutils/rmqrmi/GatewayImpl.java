package com.duspensky.jutils.rmqrmi;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.duspensky.jutils.common.CloseableHolder;
import com.duspensky.jutils.common.Misc;
import com.duspensky.jutils.common.Misc.FunctionWithException;
import com.duspensky.jutils.common.Misc.RunnableWithException;
import com.duspensky.jutils.rmqrmi.Exceptions.BadInterface;
import com.duspensky.jutils.rmqrmi.Exceptions.BadInvocation;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GatewayImpl implements Gateway {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayImpl.class);

  private static final String REQUEST_EXCHANGE = "request";

  private ExecutorService operationExecutor;
  private Map<Map.Entry<String, String>, FunctionWithException<byte[], Object>> impls = new HashMap<>();
  private Map<String, java.util.function.Consumer<byte[]>> requests = new HashMap<>();
  private Config config;
  private ConnectionFactory factory;
  private Connection connection;
  private Channel channel;
  private String ownQueue;
  private long requestCounter;

  GatewayImpl(Config cfg, String threadName) {
    operationExecutor = Misc.namedThreadExecutor(ObjectUtils.firstNonNull(threadName, "gateway-main"));
    config = cfg;
    factory = new ConnectionFactory();
    factory.setHost(config.host);
    factory.setPort(config.port);
    factory.setVirtualHost(config.vHost);
    factory.setSharedExecutor(operationExecutor);
    reconnect();
  }

  @Override
  public void close() throws Exception {
    run(this::closeImpl);
    Misc.waitAllOperationsProcessed(operationExecutor);
    Misc.shutdown(operationExecutor);
  }

  @Override
  public <T> void registerImplementation(Class<T> iface, T impl) throws BadInterface {
    checkIfaceIsAcceptable(iface);
    runExceptional(() -> registerImpl(iface, impl));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T buildClient(Class<T> iface) throws BadInterface {
    checkIfaceIsAcceptable(iface);
    Map<Method, Map.Entry<String, Class<?>>> methodDescriptors = new HashMap<>();
    for (Method method : iface.getMethods()) {
      methodDescriptors.put(method, Misc.makePair(fullName(method), method.getReturnType()));
    }
    String iName = ifaceName(iface);
    Serializer serial = config.serializer;
    boolean oneWay = isEventInterface(iface);
    return (T) Proxy.newProxyInstance(
        iface.getClassLoader(), new Class[]{iface},
        (Object proxy, Method method, Object[] args) -> {
          Map.Entry<String, Class<?>> descriptor = methodDescriptors.get(method);
          if (descriptor == null) {
            throw new BadInvocation(String.format("Interface %s has no %s method", iName, method.getName()));
          }
          if (oneWay) {
            invokeRemoteOneWay(iName, descriptor.getKey(), serial.serialize(args));
            return null;
          }
          return serial.deserialize(
            descriptor.getValue(), invokeRemote(iName, descriptor.getKey(), serial.serialize(args)).get());
        });
  }

  @Override
  public void reconnect() {
    runExceptional(this::reconnectImpl);
  }

  private void runExceptional(RunnableWithException operation) {
    run(() -> {
      try {
        operation.run();
      } catch (Exception e) {
        LOG.error("Exception", e);
      }
    });
  }

  private void run(Runnable operation) {
    operationExecutor.execute(operation);
  }

  private void reconnectImpl() throws Exception {
    LOG.debug("reconnectImpl");
    closeImpl();
    try (CloseableHolder<Connection> connHolder = new CloseableHolder<>(factory.newConnection());
         CloseableHolder<Channel> chHolder = new CloseableHolder<>(connHolder.get().createChannel())){
      Channel ch = chHolder.get();
      String queue = ch.queueDeclare().getQueue();
      ch.exchangeDeclare(REQUEST_EXCHANGE, BuiltinExchangeType.DIRECT);
      ch.basicConsume(queue, new Consumer() {
        @Override
        public void handleConsumeOk(String consumerTag) {
          // TODO Auto-generated method stub
        }

        @Override
        public void handleCancelOk(String consumerTag) {
          // TODO Auto-generated method stub

        }

        @Override
        public void handleCancel(String consumerTag) throws IOException {
          // TODO Auto-generated method stub

        }

        @Override
        public void handleDelivery(
          String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
          if (config.corellator != null) {
            config.corellator.setCorellationId(properties.getCorrelationId());
          }
          String exchangeName = envelope.getExchange();
          if (exchangeName.isEmpty()) {
            handleResponse(body, properties.getMessageId());
          } else {
            handleRequest(
              envelope.getRoutingKey(), properties.getAppId(), body,
              properties.getMessageId(), properties.getReplyTo());
          }
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
          // TODO Auto-generated method stub
        }

        @Override
        public void handleRecoverOk(String consumerTag) {
          // TODO Auto-generated method stub

        }
      });
      connection = connHolder.release();
      channel = chHolder.release();
      ownQueue = queue;
    }
  }

  private void closeImpl() {
    LOG.debug("closeImpl");
    Misc.silentClose(connection);
    connection = null;
    channel = null;
    ownQueue = null;
  }

  private void registerImpl(Class<?> iface, Object impl) throws BadInterface, IOException {
    String iName = ifaceName(iface);
    LOG.info("Registering {} as {}", impl.getClass(), iName);
    Map<Map.Entry<String, String>, FunctionWithException<byte[], Object>> ifaceImpls = new HashMap<>();
    Serializer serial = config.serializer;
    for (Method method : iface.getMethods()) {
      String name = fullName(method);
      LOG.debug("Registering {} of {}", name, iName);
      if (impls.get(Misc.makePair(iName, name)) != null) {
        throw new BadInterface(String.format("Duplicate implementation of %s %s", iName, name));
      }
      final Class<?>[] cls = method.getParameterTypes();
      ifaceImpls.put(Misc.makePair(iName, name), (byte[] data) -> {
        return method.invoke(impl, serial.deserialize(cls, data));
      });
    }
    impls.putAll(ifaceImpls);

    if (channel != null) {
      channel.queueBind(ownQueue, REQUEST_EXCHANGE, ifaceName(iface));
    }
  }

  private Future<byte[]> invokeRemote(String ifaceName, String methodName, byte[] args) {
    LOG.debug("invokeRemote {} on {}", methodName, ifaceName);
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    run(() -> {
      try {
        invokeImpl(future, ifaceName, methodName, args);
      } catch (BadInvocation e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  private void invokeRemoteOneWay(String ifaceName, String methodName, byte[] args) {
    LOG.debug("invokeRemoteOneWay {} on {}", methodName, ifaceName);
    runExceptional(() -> {
      if (channel != null) {
        sendRequest(null, ifaceName, methodName, args);
      }
    });
  }

  private void handleRequest(String ifaceName, String methodName, byte[] body, String msgId, String replyTo) {
    LOG.debug(
        "handleRequest iface={}, method={}, msgId={}, replyTo={} body_length={}", 
        ifaceName, methodName, msgId, replyTo, body.length);
    config.executor.execute(() -> {
      // TODO: send response in case of exception
      try {
        Object result = impls.get(Misc.makePair(ifaceName, methodName)).apply(body);
        if (replyTo != null) {
          byte[] response = config.serializer.serialize(result);
          runExceptional(() -> sendResponseImpl(replyTo, msgId, response));
        }
      } catch (Exception e) {
        LOG.error(String.format("Exception on %s %s", ifaceName, methodName), e);
      }
    });
  }

  private void handleResponse(byte[] data, String msgId) {
    LOG.debug("handleResponse of length={} on msgId={}", data.length, msgId);
    requests.remove(msgId).accept(data);
  }

  private void invokeImpl(
      CompletableFuture<byte[]> future, String ifaceName, String methodName, byte[] args) throws BadInvocation {
    if (channel == null) {
      throw new BadInvocation("No connection with RMQ server");
    }
    String messageId = Long.toString(++requestCounter);
    requests.put(messageId, (byte[] data) -> {
      try {
        future.complete(data);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    try {
      sendRequest(messageId, ifaceName, methodName, args);
    } catch (IOException e) {
      throw new BadInvocation("Failed to perform request", e);
    }
  }

  private void sendRequest(String messageId, String ifaceName, String methodName, byte[] args) throws IOException {
    BasicProperties props = new BasicProperties.Builder()
                                .replyTo(messageId == null ? null : ownQueue)
                                .messageId(messageId)
                                .appId(methodName)
                                .build();
    channel.basicPublish(REQUEST_EXCHANGE, ifaceName, props, args);
  }

  private void sendResponseImpl(String queue, String msgId, byte[] data) throws IOException {
    if (channel != null) {
      LOG.debug("Sending response of length={} to msgId={}", data.length, msgId);
      channel.basicPublish("", queue, new BasicProperties.Builder().messageId(msgId).build(), data);
    }
  }

  private static void checkIfaceIsAcceptable(Class<?> iface) throws BadInterface {
    if (!iface.isInterface() || !Modifier.isPublic(iface.getModifiers())) {
      throw new BadInterface(String.format("'%s' is not the public accessed interface", iface.getCanonicalName()));
    }
    if (isEventInterface(iface)) {
      for (Method method : iface.getMethods()) {
        Class<?> rt = method.getReturnType();
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

  private static String fullName(Method method) {
    StringBuilder builder = new StringBuilder();
    builder.append(method.getName());
    builder.append("#");
    Checksum crc32 = new CRC32C();
    crc32.update(method.getReturnType().getCanonicalName().getBytes());
    for (Class<?> p : method.getParameterTypes()) {
      crc32.update(p.getCanonicalName().getBytes());
    }
    builder.append(crc32.getValue());
    return builder.toString();
  }

  private static String ifaceName(Class<?> iface) {
    return iface.getCanonicalName();
  }
}
