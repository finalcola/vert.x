/*
 * Copyright (c) 2011-2017 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.core.http.impl;

import io.netty.channel.Channel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.impl.pool.Pool;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpClientMetrics;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The connection manager associates remote hosts with pools, it also tracks all connections so they can be closed
 * when the manager is closed.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
class ConnectionManager {

  private static final LongSupplier CLOCK = System::currentTimeMillis;

  private final int maxWaitQueueSize;
  private final HttpClientMetrics metrics; // Shall be removed later combining the PoolMetrics with HttpClientMetrics
  private final HttpClientImpl client;
  // 管理连接
  private final Map<Channel, HttpClientConnection> connectionMap = new ConcurrentHashMap<>();
  private final Map<EndpointKey, Endpoint> endpointMap = new ConcurrentHashMap<>();
  private final HttpVersion version;
  private final long maxSize;
  private long timerID;

  ConnectionManager(HttpClientImpl client,
                    HttpClientMetrics metrics,
                    HttpVersion version,
                    long maxSize,
                    int maxWaitQueueSize) {
    this.client = client;
    this.maxWaitQueueSize = maxWaitQueueSize;
    this.metrics = metrics;
    this.maxSize = maxSize;
    this.version = version;
  }

  synchronized void start() {
    // 默认1s
    long period = client.getOptions().getPoolCleanerPeriod();
    // 开启后台清理任务
    this.timerID = period > 0 ? client.getVertx().setTimer(period, id -> checkExpired(period)) : -1;
  }

  // 清除过期的client连接
  private synchronized void checkExpired(long period) {
    //
    endpointMap.values().forEach(e -> e.pool.closeIdle());
    timerID = client.getVertx().setTimer(period, id -> checkExpired(period));
  }

  private static final class EndpointKey {

    private final boolean ssl;
    private final SocketAddress server;
    private final SocketAddress peerAddress;

    EndpointKey(boolean ssl, SocketAddress server, SocketAddress peerAddress) {
      if (server == null) {
        throw new NullPointerException("No null host");
      }
      if (peerAddress == null) {
        throw new NullPointerException("No null peer address");
      }
      this.ssl = ssl;
      this.peerAddress = peerAddress;
      this.server = server;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EndpointKey that = (EndpointKey) o;
      return ssl == that.ssl && server.equals(that.server) && peerAddress.equals(that.peerAddress);
    }

    @Override
    public int hashCode() {
      int result = ssl ? 1 : 0;
      result = 31 * result + peerAddress.hashCode();
      result = 31 * result + server.hashCode();
      return result;
    }
  }

  class Endpoint {

    private final Pool<HttpClientConnection> pool;
    private final Object metric;

    public Endpoint(Pool<HttpClientConnection> pool, Object metric) {
      this.pool = pool;
      this.metric = metric;
    }
  }

  void getConnection(ContextInternal ctx, SocketAddress peerAddress, boolean ssl, SocketAddress server, Handler<AsyncResult<HttpClientConnection>> handler) {
    EndpointKey key = new EndpointKey(ssl, server, peerAddress);
    while (true) {
      Endpoint endpoint = endpointMap.computeIfAbsent(key, targetAddress -> {
        // 连接池大小限制
        int maxPoolSize = Math.max(client.getOptions().getMaxPoolSize(), client.getOptions().getHttp2MaxPoolSize());
        // 解析地址、端口
        String host;
        int port;
        if (server.path() == null) {
          host = server.host();
          port = server.port();
        } else {
          host = server.path();
          port = 0;
        }
        Object metric = metrics != null ? metrics.createEndpoint(host, port, maxPoolSize) : null;

        // 创建连接组件
        HttpChannelConnector connector = new HttpChannelConnector(client, metric, version, ssl, peerAddress, server);
        // 连接池
        Pool<HttpClientConnection> pool = new Pool<>(ctx, connector, CLOCK, maxWaitQueueSize, connector.weight(), maxSize,
          v -> {
            // pool关闭时回调，从endpointMap中移除
            if (metrics != null) {
              metrics.closeEndpoint(host, port, metric);
            }
            endpointMap.remove(key);
          }/*poolClosed*/,
          conn -> connectionMap.put(conn.channel(), conn)/*connectionAdded*/,
          conn -> connectionMap.remove(conn.channel(), conn)/*connectionRemoved*/,
          false);
        // 使用Endpoint封装连接池
        return new Endpoint(pool, metric);
      });
      Object metric;
      if (metrics != null) {
        metric = metrics.enqueueRequest(endpoint.metric);
      } else {
        metric = null;
      }

      // 获取连接并通知回调
      if (endpoint.pool.getConnection(ar -> {
        // 获取连接成功后的回调
        if (metrics != null) {
          metrics.dequeueRequest(endpoint.metric, metric);
        }
        handler.handle(ar);
      })) {
        break;
      }
    }
  }

  public void close() {
    synchronized (this) {
      if (timerID >= 0) {
        client.getVertx().cancelTimer(timerID);
        timerID = -1;
      }
    }
    endpointMap.clear();
    for (HttpClientConnection conn : connectionMap.values()) {
      conn.close();
    }
  }
}
