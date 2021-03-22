package com.duspensky.jutils.rmi;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import com.duspensky.jutils.common.Misc;
import com.duspensky.jutils.rmi.Exceptions.BadInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ImplementationRegistrator {
  private static final Logger LOG = LoggerFactory.getLogger(ImplementationRegistrator.class);
  
  interface NameProvider {
    String getName(Class<?> iface);
    String getName(Method method);
  }

  static class TargetDescriptor {
    final Object obj;
    final Executor executor;

    TargetDescriptor(Object o, Executor e) {
      obj = o;
      executor = e;
    }
  }

  static class MethodDescriptor {
    final Method method;
    final TargetDescriptor target;

    MethodDescriptor(Method m, TargetDescriptor t) {
      method = m;
      target = t;
    }
  }

  private Map<Map.Entry<String, String>, MethodDescriptor> methods = new HashMap<>();
  private NameProvider nameProvider;

  ImplementationRegistrator(NameProvider provider) {
    nameProvider = provider;
  }

  public void register(Class<?> iface, Object impl, Executor ex) throws BadInterface {
    var ifaceName = nameProvider.getName(iface);
    LOG.info("Registering {} as {}", impl.getClass(), ifaceName);
    var ifaceMethods = new HashMap<Map.Entry<String, String>, MethodDescriptor>();
    var targetDescr = new TargetDescriptor(impl, ex);
    for (var method : iface.getMethods()) {
      var methodName = nameProvider.getName(method);
      LOG.debug("Registering {} of {}", methodName, ifaceName);
      var key = Misc.makePair(ifaceName, methodName);
      if (methods.get(key) != null) {
        throw new BadInterface(String.format("Duplicate implementation of %s %s", ifaceName, methodName));
      }
      ifaceMethods.put(key, new MethodDescriptor(method, targetDescr));
    }
    methods.putAll(ifaceMethods);
  }

  public MethodDescriptor get(String ifaceName, String methodName) {
    return methods.get(Misc.makePair(ifaceName, methodName));
  }
}
