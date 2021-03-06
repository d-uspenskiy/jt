package com.duspensky.jutils.rmqrmi;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.duspensky.jutils.common.ExecutorAsService;
import com.duspensky.jutils.common.ThreadExecutor;
import com.duspensky.jutils.common.Misc.FunctionWithException;
import com.duspensky.jutils.common.Misc.RunnableWithException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

import static com.duspensky.jutils.common.Misc.silentClose;
import static com.duspensky.jutils.common.Misc.makePair;
import static com.duspensky.jutils.rmqrmi.Exceptions.BadInterface;
import static com.duspensky.jutils.rmqrmi.Exceptions.BadInvocation;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GatewayImpl implements Gateway {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayImpl.class);

  private static final String REQUEST_EXCHANGE = "request";
  private static final String EVENT_EXCHANGE = "event";

  private ThreadExecutor operationExecutor_;
  private ExecutorService consumerExecutor_;
  private Map<Map.Entry<String, String>, FunctionWithException<byte[], byte[]>> impls_ = new HashMap<>();
  private Map<String, java.util.function.Consumer<byte[]>> requests_ = new HashMap<>();
  private Config config_;
  private ConnectionFactory factory_;
  private Connection connection_;
  private Channel channel_;
  private String ownQueue_;
  private long requestCounter_;

  GatewayImpl(Config config, String threadName) {
    operationExecutor_ = new ThreadExecutor(ObjectUtils.firstNonNull(threadName, "gateway-main"));
    consumerExecutor_ = new ExecutorAsService(operationExecutor_);
    config_ = config;
    factory_ = new ConnectionFactory();
    factory_.setHost(config_.host);
    factory_.setPort(config_.port);
    factory_.setVirtualHost(config_.vHost);
    factory_.setSharedExecutor(consumerExecutor_);
    reconnect();
  }

  @Override
  public void close() throws Exception {
    run(this::closeImpl);
    operationExecutor_.close();
  }

  @Override
  public <T> void register(Class<T> cl, T impl) throws BadInterface {
    checkClassIsAcceptable(cl);
    runExceptional(() -> registerImpl(cl, impl));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T stub(Class<T> cl) throws BadInterface {
    checkClassIsAcceptable(cl);
    Map<Method, Map.Entry<String, Class<?>>> methodDescriptors = new HashMap<>();
    for (Method method : cl.getMethods()) {
      methodDescriptors.put(method, makePair(fullName(method), method.getReturnType()));
    }
    String ifaceName = cl.getCanonicalName();
    Serializer serial = config_.serializer;
    return (T) Proxy.newProxyInstance(
        cl.getClassLoader(), new Class[] { cl },
        (Object proxy, Method method, Object[] args) -> {
          Map.Entry<String, Class<?>> descriptor = methodDescriptors.get(method);
          if (descriptor == null) {
            throw new BadInvocation(String.format("Interface %s has no %s method", ifaceName, method.getName()));
          }
          return serial.deserialize(
            descriptor.getValue(), invokeRemote(ifaceName, descriptor.getKey(), serial.serialize(args)).get());
        });
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
    operationExecutor_.execute(operation);
  }

  @Override
  public void reconnect() {
    runExceptional(this::reconnectImpl);
  }

  private void reconnectImpl() throws IOException, TimeoutException {
    LOG.debug("reconnectImpl");
    closeImpl();
    Connection connection = null;
    Channel channel = null;
    try {
      connection = factory_.newConnection();
      channel = connection.createChannel();
      String queue = channel.queueDeclare().getQueue();
      channel.exchangeDeclare(REQUEST_EXCHANGE, BuiltinExchangeType.DIRECT);
      channel.basicConsume(queue, new Consumer() {
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
          if (config_.corellator != null) {
            config_.corellator.setCorellationId(properties.getCorrelationId());
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
      connection_ = connection;
      channel_ = channel;
      ownQueue_ = queue;
      connection = null;
      channel = null;
    } finally {
      silentClose(channel);
      silentClose(connection);
    }
  }

  private void closeImpl() {
    LOG.debug("closeImpl");
    silentClose(channel_);
    silentClose(connection_);
    connection_ = null;
    channel_ = null;
    ownQueue_ = null;
  }

  private void registerImpl(Class<?> cl, Object impl) throws BadInterface, IOException {
    String ifaceName = cl.getCanonicalName();
    LOG.info("Registering {} as {}", impl.getClass(), ifaceName);
    Map<Map.Entry<String, String>, FunctionWithException<byte[], byte[]>> ifaceImpls = new HashMap<>();
    Serializer serial = config_.serializer;
    for (Method m : cl.getMethods()) {
      String name = fullName(m);
      LOG.debug("Registering {} of {}", name, ifaceName);
      if (impls_.get(makePair(ifaceName, name)) != null) {
        throw new BadInterface(String.format("Duplicate implementation of %s %s", ifaceName, name));
      }
      final Class<?>[] cls = m.getParameterTypes();
      ifaceImpls.put(makePair(ifaceName, name), (byte[] data) -> {
        return serial.serialize(m.invoke(impl, serial.deserialize(cls, data)));
      });
    }
    impls_.putAll(ifaceImpls);

    if (channel_ != null) {
      channel_.queueBind(ownQueue_, REQUEST_EXCHANGE, ifaceName);
    }
  }

  private Future<byte[]> invokeRemote(String ifaceName, String methodName, byte[] args) {
    LOG.debug("invoke {} on {}", methodName, ifaceName);
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

  private void handleRequest(String ifaceName, String methodName, byte[] body, String msgId, String replyTo) {
    LOG.debug(
        "handleRequest iface={}, method={}, msgId={}, replyTo={} body_length={}", 
        ifaceName, methodName, msgId, replyTo, body.length);
    config_.executor.execute(() -> {
      // TODO: send response in case of exception
      try {
        byte[] response = impls_.get(makePair(ifaceName, methodName)).apply(body);
        runExceptional(() -> sendResponseImpl(replyTo, msgId, response));
      } catch (Exception e) {
        LOG.error(String.format("Exception on %s %s", ifaceName, methodName), e);
      }
    });
  }

  private void handleResponse(byte[] data, String msgId) {
    LOG.debug("handleResponse of length={} on msgId={}", data.length, msgId);
    requests_.remove(msgId).accept(data);
  }

  private void invokeImpl(
        CompletableFuture<byte[]> future, String ifaceName, String methodName, byte[] args) throws BadInvocation {
    if (channel_ == null) {
      throw new BadInvocation("No connection with RMQ server");
    }
    final String messageId = Long.toString(++requestCounter_);
    requests_.put(messageId, (byte[] data) -> {
      try {
        future.complete(data);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    BasicProperties props = new BasicProperties.Builder()
                                .replyTo(ownQueue_)
                                .messageId(messageId)
                                .appId(methodName)
                                .build();
    try {
      channel_.basicPublish(REQUEST_EXCHANGE, ifaceName, props, args);
    } catch (IOException e) {
      throw new BadInvocation("Failed to perform request", e);
    }
  }

  private void sendResponseImpl(String queue, String msgId, byte[] data) throws IOException {
    if (channel_ != null) {
      LOG.debug("Sending response of length={} to msgId={}", data.length, msgId);
      channel_.basicPublish("", queue, new BasicProperties.Builder().messageId(msgId).build(), data);
    }
  }

  private void checkClassIsAcceptable(Class<?> cl) throws BadInterface {
    if (!cl.isInterface() || !Modifier.isPublic(cl.getModifiers())) {
      throw new BadInterface(String.format("%s is not the public accessed interface", cl.getCanonicalName()));
    }
  }

  private String fullName(Method method) {
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
}
