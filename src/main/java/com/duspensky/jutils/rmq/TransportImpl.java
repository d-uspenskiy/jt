package com.duspensky.jutils.rmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.duspensky.jutils.rmi.transport.Exceptions.BadTopic;
import com.duspensky.jutils.rmi.transport.Exceptions.BrokenConnection;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.duspensky.jutils.common.Misc;
import com.duspensky.jutils.rmi.transport.Processor;
import com.duspensky.jutils.rmi.transport.Reply;
import com.duspensky.jutils.rmi.transport.Transport;

class TransportImpl implements Transport, com.rabbitmq.client.Consumer {
  private static final Logger LOG = LoggerFactory.getLogger(TransportImpl.class);
  private static final String EXCHANGE_REQUEST = "request";

  private final ConnectionFactory factory;
  private final Set<String> subscriptions = new HashSet<>();
  private final Map<String, Consumer<Reply>> pendingRequests = new HashMap<>();
  private final Processor messageProcessor;
  private Connection connection;
  private Channel channel;
  private String ownQueue;
  private int nextRequestId;

  public TransportImpl(Config config) {
    messageProcessor = config.processor;
    factory = new ConnectionFactory();
    factory.setHost(config.host);
    factory.setPort(config.port);
    factory.setVirtualHost(config.vHost);
    factory.setSharedExecutor(config.executor);
  }

  @Override
  public void subscribe(String topic) throws BadTopic {
    LOG.info("subscribe topic='{}'", topic);
    if (!subscriptions.add(topic)) {
      throw new BadTopic(String.format("Topic '%s' already exists", topic));
    }
    if (channel != null) {
      try {
        channel.queueBind(ownQueue, EXCHANGE_REQUEST, topic);
      } catch (IOException e) {
        LOG.error("Exception", e);
      }
    }
  }

  @Override
  public void unsubscribe(String topic) throws BadTopic {
    LOG.info("unsubscribe topic='{}'", topic);
    if (!subscriptions.remove(topic)) {
      throw new BadTopic(String.format("Unknown topic '%s'", topic));
    }
    if (channel != null) {
      try {
        channel.queueUnbind(ownQueue, EXCHANGE_REQUEST, topic);
      } catch (IOException e) {
        LOG.error("Exception", e);
      }
    }
  }

  @Override
  public void send(String topic, String subTopic, byte[] data, Consumer<Reply> consumer) throws BrokenConnection {
    LOG.debug("send topic='{}' subTopic='{}', dataLen={}", topic, subTopic, data.length);
    if (channel == null) {
      throw new BrokenConnection("No connection");
    }
    String messageId = null;
    String replyTo = null;
    if (consumer != null) {
      var requestId = ++nextRequestId;
      messageId = Integer.toString(requestId);
      replyTo = ownQueue;
    }
    var props = new BasicProperties.Builder().replyTo(replyTo).messageId(messageId).appId(subTopic).build();
    try {
      channel.basicPublish(EXCHANGE_REQUEST, topic, props, data);
    } catch (IOException e) {
      throw new BrokenConnection("Sending failed", e);
    }
    if (messageId != null) {
      pendingRequests.put(messageId, consumer);
    }
  }

  @Override
  public void activate() throws BrokenConnection {
    LOG.info("activate");
    if (channel == null) {
      try {
        activateImpl();
      } catch (Exception e) {
        throw new BrokenConnection("Connection failed", e);
      }
    }
  }

  @Override
  public void deactivate() {
    LOG.info("deactivate");
    Connection c = connection;
    connection = null;
    channel = null;
    ownQueue = null;
    Misc.silentClose(c);
  }

  @SuppressWarnings("java:S1186")
  @Override
  public void handleConsumeOk(String consumerTag) {}

  @SuppressWarnings("java:S1186")
  @Override
  public void handleCancelOk(String consumerTag) {}

  @SuppressWarnings("java:S1186")
  @Override
  public void handleCancel(String consumerTag) throws IOException {}

  @SuppressWarnings("java:S1186")
  @Override
  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {}

  @SuppressWarnings("java:S1186")
  @Override
  public void handleRecoverOk(String consumerTag) {}

  @Override
  public void handleDelivery(
      String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
    var messageId = properties.getMessageId();
    if (envelope.getExchange().isEmpty()) {
      handleResponse(messageId, body);
    } else {
      handleRequest(envelope.getRoutingKey(), properties.getAppId(), messageId, properties.getReplyTo(), body);
    }
  }

  @SuppressWarnings("java:S1181")
  private void handleRequest(String topic, String subTopic, String messageId, String replyTo, byte[] body) {
    LOG.debug(
      "handleRequest topic='{}' subTopic='{}' messageId='{}' replyTo='{}' bodyLen={}",
      topic, subTopic, messageId, replyTo, body.length);
    Consumer<byte[]> replyHandler = null;
    if (replyTo != null) {
      replyHandler = response -> {
        try {
          channel.basicPublish("", replyTo, new BasicProperties.Builder().messageId(messageId).build(), response);
        } catch (Throwable e) {
          LOG.error(String.format("Exception while sending reply on '%s'", messageId), e);
        }
      };
    }
    messageProcessor.onMessage(topic, subTopic, body, replyHandler);
  }

  private void handleResponse(String messageId, byte[] body) {
    LOG.debug("handleResponse messageId='{}' bodyLen={}", messageId, body.length);
    var handler = pendingRequests.remove(messageId);
    if (handler != null) {
      handler.accept(new Reply(body, null));
    } else {
      LOG.warn("Unexpected response '{}'", messageId);
    }
  }

  private void activateImpl() throws Exception {
    try (var connectionHolder = Misc.closeableHolder(factory.newConnection());
         var channelHolder = Misc.closeableHolder(connectionHolder.get().createChannel())) {
      Channel ch = channelHolder.get();
      String queue = ch.queueDeclare().getQueue();
      ch.exchangeDeclare(EXCHANGE_REQUEST, BuiltinExchangeType.DIRECT);
      ch.basicConsume(queue, this);
      subscribeAll();
      connection = connectionHolder.release();
      channel = channelHolder.release();
      ownQueue = queue;
    }
  }

  private void subscribeAll() {
    LOG.debug("subscribeAll NIY");
  }
}
