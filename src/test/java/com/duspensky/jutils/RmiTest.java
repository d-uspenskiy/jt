package com.duspensky.jutils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import com.duspensky.jutils.rmi.Exceptions.BadInterface;
import com.duspensky.jutils.rmi.transport.Exceptions.BadTopic;
import com.duspensky.jutils.rmi.transport.Processor;
import com.duspensky.jutils.rmi.transport.Reply;
import com.duspensky.jutils.rmi.transport.Transport;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RmiTest {
  private static final Logger LOG = LoggerFactory.getLogger(RmiTest.class);

  public interface Calculator {
    Integer mul(Integer a, Integer b);
  }

  private class CalculatorImpl implements Calculator {

    @Override
    public Integer mul(Integer a, Integer b) {
      LOG.debug("mul {} {}", a, b);
      return a * b;
    }
  }

  private static class BasicTransport implements Transport {
    private final Set<String> subscriptions = new HashSet<>();
    private final Processor processor;
    private final Executor executor;

    BasicTransport(Processor p, Executor e) {
      processor = p;
      executor = e;
    }

    @Override
    public void subscribe(String topic) throws BadTopic {
      if (!subscriptions.add(topic))
        throw new BadTopic("Existing topic");
    }

    @Override
    public void unsubscribe(String topic) throws BadTopic {
      if (!subscriptions.remove(topic))
        throw new BadTopic("Unknown topic");
    }

    @Override
    public void send(String topic, String subTopic, byte[] data, Consumer<Reply> consumer) {
      executor.execute(() -> {
        processor.onMessage(topic, subTopic, data, response -> consumer.accept(new Reply(response, null)));
      });
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }
  }

  @Test
  public void testRegister() throws Exception {
    LOG.info("*** testRegister ***");
    try (var helper = getewayHelper()) {
      helper.registrator.register(Calculator.class, new CalculatorImpl());
      try {
        helper.registrator.register(Calculator.class, new CalculatorImpl());
        assert(false);
      } catch (BadInterface e) {
        LOG.info("Excepcted exception", e);
      }
    }
  }

  @Test
  public void testCommunication() throws Exception {
    LOG.info("*** testCommunication ***");
    try (var helper = getewayHelper()) {
      helper.registrator.register(Calculator.class, new CalculatorImpl());
      var calc = helper.gateway.buildClient(Calculator.class);
      var result = calc.mul(2, 2);
      assertEquals(4, result);
    }
  }

  private TestUtils.GatewayHelper getewayHelper() {
    return TestUtils.gatewayHelper((processor, executor) -> new BasicTransport(processor, executor),
                                   TestUtils.buildWorkerExecutor(),
                                   TestUtils.workerThreadChecker());
  }
}
