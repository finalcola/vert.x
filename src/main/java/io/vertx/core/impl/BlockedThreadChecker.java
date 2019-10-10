/*
 * Copyright (c) 2014 Red Hat, Inc. and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.core.impl;

import io.vertx.core.VertxException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BlockedThreadChecker {

  /**
   * A checked task.
   */
  public interface Task {
    long startTime();
    long maxExecTime();
    TimeUnit maxExecTimeUnit();
  }

  private static final Logger log = LoggerFactory.getLogger(BlockedThreadChecker.class);

  private final Map<Thread, Task> threads = new WeakHashMap<>();
  private final Timer timer; // Need to use our own timer - can't use event loop for this

  BlockedThreadChecker(long interval, TimeUnit intervalUnit, long warningExceptionTime, TimeUnit warningExceptionTimeUnit) {
    // 新建timer，周期性检查阻塞线程
    timer = new Timer("vertx-blocked-thread-checker", true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        synchronized (BlockedThreadChecker.this) {
          long now = System.nanoTime();
          for (Map.Entry<Thread, Task> entry : threads.entrySet()) {
            // 线程开启时间
            long execStart = entry.getValue().startTime();
            // 运行时间
            long dur = now - execStart;
            // 执行时间
            final long timeLimit = entry.getValue().maxExecTime();
            TimeUnit maxExecTimeUnit = entry.getValue().maxExecTimeUnit();
            // 运行时间
            long val = maxExecTimeUnit.convert(dur, TimeUnit.NANOSECONDS);
            // 运行时间超过maxExecTime
            if (execStart != 0 && val >= timeLimit) {
              final String message = "Thread " + entry.getKey() + " has been blocked for " + (dur / 1_000_000) + " ms, time limit is " + TimeUnit.MILLISECONDS.convert(timeLimit, maxExecTimeUnit) + " ms";
              // 运行时间小于warningExceptionTimeUnit
              if (warningExceptionTimeUnit.convert(dur, TimeUnit.NANOSECONDS) <= warningExceptionTime) {
                // 打印线程阻塞时间的日志
                log.warn(message);
              } else {
                // 打印线程阻塞时间的日志并打印调用栈
                VertxException stackTrace = new VertxException("Thread blocked");
                stackTrace.setStackTrace(entry.getKey().getStackTrace());
                log.warn(message, stackTrace);
              }
            }
          }
        }
      }
    }, intervalUnit.toMillis(interval), intervalUnit.toMillis(interval));
  }

  // 注册线程
  synchronized void registerThread(Thread thread, Task checked) {
    threads.put(thread, checked);
  }

  public void close() {
    timer.cancel();
  }
}
