package com.duspensky.jutils.common;

import java.util.ArrayList;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadExecutor implements Executor, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadExecutor.class);

  private ArrayList<Runnable> cmds_ = new ArrayList<>();
  private ArrayList<Runnable> shadow_ = new ArrayList<>();
  private boolean stoped_ = false;
  private Thread thread_;
  private Object lock_ = new Object();

  public ThreadExecutor() {
    thread_ = new Thread(this::process);
    thread_.start();
  }

  @Override
  public void close() throws Exception {
    execute(() -> stoped_ = true);
    thread_.join();
  }

  @Override
  public void execute(Runnable cmd) {
    synchronized (lock_) {
      cmds_.add(cmd);
      lock_.notifyAll();
    }
  }

  private void process() {
    try {
      while (!stoped_) {
        synchronized (lock_) {
          while (cmds_.isEmpty()) {
            lock_.wait();
          }
          ArrayList<Runnable> tmp = cmds_;
          cmds_ = shadow_;
          shadow_ = tmp;
        }
        for (Runnable r : shadow_) {
          try {
            r.run();
          } catch (Exception e) {
            LOG.error("Operation exception", e);
          }
        }
        shadow_.clear();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
