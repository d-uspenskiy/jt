package com.duspensky.jutils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

import com.duspensky.jutils.common.ExecutorHolder;
import com.duspensky.jutils.common.Misc;
import com.duspensky.jutils.rmi.EventInterface;
import com.duspensky.jutils.rmq.TransportFactoryRMQ;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RmiRmqTest {
  private static final Logger LOG = LoggerFactory.getLogger(RmiRmqTest.class);

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

  @Test
  public void testInvocation() throws Exception {
    LOG.info("*** testInvocation ***");

    var factory = new TransportFactoryRMQ();
    var wockerChecker = TestUtils.workerThreadChecker();
    try (var exec = new ExecutorHolder(TestUtils.buildWorkerExecutor());
         var gwHelper = TestUtils.gatewayHelper(factory, exec.get(), wockerChecker);
         var extraGwHelper = TestUtils.gatewayHelper(factory, exec.get(), wockerChecker)) {
      var gwResult = new ArrayList<Integer>();
      var extraGwResult = new ArrayList<Integer>();
      gwHelper.registrator.register(Operations.class, new OperationsImpl());
      gwHelper.registrator.register(Notification.class, new NotificationImpl(gwResult));
      extraGwHelper.registrator.register(Notification.class, new NotificationImpl(extraGwResult));
      Operations opService = gwHelper.gateway.buildClient(Operations.class);
      Notification ntfService = gwHelper.gateway.buildClient(Notification.class);
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
