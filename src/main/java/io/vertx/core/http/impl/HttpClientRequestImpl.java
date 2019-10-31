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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.StreamPriority;
import io.vertx.core.http.impl.headers.VertxHttpHeaders;
import io.vertx.core.impl.Arguments;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.vertx.core.http.HttpHeaders.*;

/**
 * This class is optimised for performance when used on the same event loop that is was passed to the handler with.
 * However it can be used safely from other threads.
 *
 * The internal state is protected using the synchronized keyword. If always used on the same event loop, then
 * we benefit from biased locking which makes the overhead of synchronized near zero.
 *
 * This class uses {@code this} for synchronization purpose. The {@link #client}  or{@link #stream} instead are
 * called must not be called under this lock to avoid deadlocks.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class HttpClientRequestImpl extends HttpClientRequestBase implements HttpClientRequest {

  static final Logger log = LoggerFactory.getLogger(HttpClientRequestImpl.class);

  private final VertxInternal vertx;
  // 默认false
  private boolean chunked;
  private String hostHeader;
  private String rawMethod;
  private Handler<Void> continueHandler;
  private Handler<Void> drainHandler;
  private Handler<HttpClientRequest> pushHandler;
  private Handler<HttpConnection> connectionHandler;
  private Handler<Throwable> exceptionHandler;
  private Promise<Void> endPromise = Promise.promise();
  private Future<Void> endFuture = endPromise.future();
  // 是否请求已完成
  private boolean ended;
  private Throwable reset;
  // 请求数据的暂存缓冲区
  private ByteBuf pendingChunks;
  // 每次调用write时传入的handler
  private List<Handler<AsyncResult<Void>>> pendingHandlers;
  private int pendingMaxSize = -1;
  private int followRedirects;
  private VertxHttpHeaders headers;
  private StreamPriority priority;
  public HttpClientStream stream;
  // 是否连接server
  private boolean connecting;

  HttpClientRequestImpl(HttpClientImpl client, boolean ssl, HttpMethod method, SocketAddress server,
                        String host, int port,
                        String relativeURI, VertxInternal vertx) {
    super(client, ssl, method, server, host, port, relativeURI);
    this.chunked = false;
    this.vertx = vertx;
    this.priority = HttpUtils.DEFAULT_STREAM_PRIORITY;
  }

  @Override
  void handleException(Throwable t) {
    super.handleException(t);
    Handler<Throwable> handler;
    synchronized (this) {
      if (exceptionHandler != null && !endFuture.isComplete()) {
        handler = exceptionHandler;
      } else {
        handler = log::error;
      }
    }
    handler.handle(t);
    endPromise.tryFail(t);
    responsePromise.tryFail(t);
  }

  @Override
  public synchronized int streamId() {
    return stream == null ? -1 : stream.id();
  }

  @Override
  public HttpClientRequest setFollowRedirects(boolean followRedirects) {
    synchronized (this) {
      checkEnded();
      if (followRedirects) {
        this.followRedirects = client.getOptions().getMaxRedirects() - 1;
      } else {
        this.followRedirects = 0;
      }
      return this;
    }
  }

  @Override
  public HttpClientRequest setMaxRedirects(int maxRedirects) {
    Arguments.require(maxRedirects >= 0, "Max redirects must be >= 0");
    synchronized (this) {
      checkEnded();
      followRedirects = maxRedirects;
      return this;
    }
  }

  @Override
  public HttpClientRequestImpl setChunked(boolean chunked) {
    synchronized (this) {
      checkEnded();
      if (stream != null) {
        throw new IllegalStateException("Cannot set chunked after data has been written on request");
      }
      // HTTP 1.0 does not support chunking so we ignore this if HTTP 1.0
      if (client.getOptions().getProtocolVersion() != io.vertx.core.http.HttpVersion.HTTP_1_0) {
        this.chunked = chunked;
      }
      return this;
    }
  }

  @Override
  public synchronized boolean isChunked() {
    return chunked;
  }

  @Override
  public synchronized String getRawMethod() {
    return rawMethod;
  }

  @Override
  public synchronized HttpClientRequest setRawMethod(String method) {
    this.rawMethod = method;
    return this;
  }

  @Override
  public synchronized HttpClientRequest setHost(String host) {
    this.hostHeader = host;
    return this;
  }

  @Override
  public synchronized String getHost() {
    return hostHeader;
  }

  @Override
  public synchronized MultiMap headers() {
    if (headers == null) {
      headers = new VertxHttpHeaders();
    }
    return headers;
  }

  @Override
  public synchronized HttpClientRequest putHeader(String name, String value) {
    checkEnded();
    headers().set(name, value);
    return this;
  }

  @Override
  public synchronized HttpClientRequest putHeader(String name, Iterable<String> values) {
    checkEnded();
    headers().set(name, values);
    return this;
  }

  @Override
  public HttpClientRequest setWriteQueueMaxSize(int maxSize) {
    HttpClientStream s;
    synchronized (this) {
      checkEnded();
      if ((s = stream) == null) {
        pendingMaxSize = maxSize;
        return this;
      }
    }
    s.doSetWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    HttpClientStream s;
    synchronized (this) {
      checkEnded();
      if ((s = stream) == null) {
        // Should actually check with max queue size and not always blindly return false
        return false;
      }
    }
    return s.isNotWritable();
  }

  private synchronized Handler<Throwable> exceptionHandler() {
    return exceptionHandler;
  }

  public synchronized HttpClientRequest exceptionHandler(Handler<Throwable> handler) {
    if (handler != null) {
      checkEnded();
      this.exceptionHandler = handler;
    } else {
      this.exceptionHandler = null;
    }
    return this;
  }

  @Override
  public HttpClientRequest drainHandler(Handler<Void> handler) {
    synchronized (this) {
      if (handler != null) {
        checkEnded();
        drainHandler = handler;
        HttpClientStream s;
        if ((s = stream) == null) {
          return this;
        }
        s.getContext().runOnContext(v -> {
          synchronized (HttpClientRequestImpl.this) {
            if (!stream.isNotWritable()) {
              handleDrained();
            }
          }
        });
      } else {
        drainHandler = null;
      }
      return this;
    }
  }

  @Override
  public synchronized HttpClientRequest continueHandler(Handler<Void> handler) {
    if (handler != null) {
      checkEnded();
    }
    this.continueHandler = handler;
    return this;
  }

  @Override
  public HttpClientRequest sendHead() {
    return sendHead(null);
  }

  @Override
  public synchronized HttpClientRequest sendHead(Handler<HttpVersion> headersHandler) {
    checkEnded();
    checkResponseHandler();
    if (stream != null) {
      throw new IllegalStateException("Head already written");
    } else {
      connect(headersHandler);
    }
    return this;
  }

  @Override
  public synchronized HttpClientRequest putHeader(CharSequence name, CharSequence value) {
    checkEnded();
    headers().set(name, value);
    return this;
  }

  @Override
  public synchronized HttpClientRequest putHeader(CharSequence name, Iterable<CharSequence> values) {
    checkEnded();
    headers().set(name, values);
    return this;
  }

  @Override
  public synchronized HttpClientRequest pushHandler(Handler<HttpClientRequest> handler) {
    pushHandler = handler;
    return this;
  }

  @Override
  boolean reset(Throwable cause) {
    HttpClientStream s;
    synchronized (this) {
      if (reset != null) {
        return false;
      }
      reset = cause;
      s = stream;
    }
    if (s != null) {
      s.reset(cause);
    } else {
      handleException(cause);
    }
    return true;
  }

  private void tryComplete() {
    endPromise.tryComplete();
  }

  @Override
  public HttpConnection connection() {
    HttpClientStream s;
    synchronized (this) {
      if ((s = stream) == null) {
        return null;
      }
    }
    return s.connection();
  }

  @Override
  public synchronized HttpClientRequest connectionHandler(@Nullable Handler<HttpConnection> handler) {
    connectionHandler = handler;
    return this;
  }

  @Override
  public synchronized HttpClientRequest writeCustomFrame(int type, int flags, Buffer payload) {
    HttpClientStream s;
    synchronized (this) {
      checkEnded();
      if ((s = stream) == null) {
        throw new IllegalStateException("Not yet connected");
      }
    }
    s.writeFrame(type, flags, payload.getByteBuf());
    return this;
  }

  // 通知producer可以继续写入
  void handleDrained() {
    Handler<Void> handler;
    synchronized (this) {
      if ((handler = drainHandler) == null || endFuture.isComplete()) {
        return;
      }
    }
    try {
      handler.handle(null);
    } catch (Throwable t) {
      handleException(t);
    }
  }

  // 处理下一个请求
  private void handleNextRequest(HttpClientRequest next, long timeoutMs) {
    // 复制当前req的属性
    next.setHandler(responsePromise.future().getHandler());
    next.exceptionHandler(exceptionHandler());
    // help gc
    exceptionHandler(null);
    next.pushHandler(pushHandler);
    // 重定向限制-1
    next.setMaxRedirects(followRedirects - 1);
    if (next.getHost() == null) {
      next.setHost(hostHeader);
    }
    if (headers != null) {
      next.headers().addAll(headers);
    }
    endFuture.setHandler(ar -> {
      if (ar.succeeded()) {
        if (timeoutMs > 0) {
          next.setTimeout(timeoutMs);
        }
        // 开启请求
        next.end();
      } else {
        next.reset(0);
      }
    });
  }

  // 处理重定向
  void handleResponse(HttpClientResponse resp, long timeoutMs) {
    if (reset == null) {
      int statusCode = resp.statusCode();
      // 重定向
      if (followRedirects > 0 && statusCode >= 300 && statusCode < 400) {
        // 通知重定向相关的handler处理，重新创建request
        Future<HttpClientRequest> next = client.redirectHandler().apply(resp);
        if (next != null) {
          next.setHandler(ar -> {
            if (ar.succeeded()) {
              // 发送下次请求
              handleNextRequest(ar.result(), timeoutMs);
            } else {
              responsePromise.fail(ar.cause());
            }
          });
          return;
        }
      }
      responsePromise.complete(resp);
    }
  }

  @Override
  protected String hostHeader() {
    return hostHeader != null ? hostHeader : super.hostHeader();
  }

  // 创建连接
  private synchronized void connect(Handler<HttpVersion> headersHandler) {
    if (!connecting) {

      if (method == HttpMethod.OTHER && rawMethod == null) {
        throw new IllegalStateException("You must provide a rawMethod when using an HttpMethod.OTHER method");
      }

      SocketAddress peerAddress;
      // 解析host
      if (hostHeader != null) {
        int idx = hostHeader.lastIndexOf(':');
        if (idx != -1) {
          peerAddress = SocketAddress.inetSocketAddress(Integer.parseInt(hostHeader.substring(idx + 1)), hostHeader.substring(0, idx));
        } else {
          peerAddress = SocketAddress.inetSocketAddress(80, hostHeader);
        }
      } else {
        String peerHost = host;
        if (peerHost.endsWith(".")) {
          peerHost = peerHost.substring(0, peerHost.length() -  1);
        }
        peerAddress = SocketAddress.inetSocketAddress(port, peerHost);
      }

      // Capture some stuff
      // 融合内外部设置的connectionHandler
      Handler<HttpConnection> h1 = connectionHandler;
      Handler<HttpConnection> h2 = client.connectionHandler();
      Handler<HttpConnection> initializer;
      if (h1 != null) {
        if (h2 != null) {
          initializer = conn -> {
            h1.handle(conn);
            h2.handle(conn);
          };
        } else {
          initializer = h1;
        }
      } else {
        initializer = h2;
      }
      // 创建context
      ContextInternal connectCtx = vertx.getOrCreateContext();



      // We defer actual connection until the first part of body is written or end is called
      // This gives the user an opportunity to set an exception handler before connecting so
      // they can capture any exceptions on connection
      // 推迟连接server，直到body第一部分写入或end方法被调用
      // 这样用户可以在连接前设置exception handler.
      connecting = true;
      // 通过client建立连接
      client.getConnectionForRequest(connectCtx, peerAddress, ssl, server, ar1 -> {
        if (ar1.succeeded()) {
          // 创建netty连接和stream成功后的回调

          HttpClientStream stream = ar1.result();
          ContextInternal ctx = stream.getContext();
          if (stream.id() == 1 && initializer != null) {
            // 通知回调
            ctx.executeFromIO(v -> {
              initializer.handle(stream.connection());
            });
          }
          // No need to synchronize as the thread is the same that set exceptionOccurred to true
          // exceptionOccurred=true getting the connection => it's a TimeoutException
          if (reset != null) {
            stream.reset(reset);
          } else {
            // 已连接，发送请求
            connected(headersHandler, stream);
          }
        } else {
          handleException(ar1.cause());
        }
      });
    }
  }

  // 连接创建好后，发送请求
  private void connected(Handler<HttpVersion> headersHandler, HttpClientStream stream) {
    synchronized (this) {
      this.stream = stream;
      // 发送前，校验并设置请求实例
      stream.beginRequest(this);

      // If anything was written or the request ended before we got the connection, then
      // we need to write it now

      if (pendingMaxSize != -1) {
        stream.doSetWriteQueueMaxSize(pendingMaxSize);
      }

      // 发送暂存的数据
      ByteBuf pending = null;
      Handler<AsyncResult<Void>> handler = null;
      if (pendingChunks != null) {
        // 回调write方法传入的handler
        List<Handler<AsyncResult<Void>>> handlers = pendingHandlers;
        pendingHandlers = null;
        pending = pendingChunks;
        pendingChunks = null;
        if (handlers != null) {
          handler = ar -> {
            handlers.forEach(h -> h.handle(ar));
          };
        }
      }
      // 写入暂存的数据
      stream.writeHead(method, rawMethod, uri, headers, hostHeader(), chunked, pending, ended, priority, continueHandler, handler);
      if (ended) {
        // we also need to write the head so optimize this and write all out in once
        // 发送请求结束，更新状态、统计数据
        stream.endRequest();
        tryComplete();
      }
      this.connecting = false;
      this.stream = stream;
    }
    if (headersHandler != null) {
      headersHandler.handle(stream.version());
    }
  }

  @Override
  public Future<Void> end(String chunk) {
    Promise<Void> promise = Promise.promise();
    end(chunk, promise);
    return promise.future();
  }

  @Override
  public void end(String chunk, Handler<AsyncResult<Void>> handler) {
    end(Buffer.buffer(chunk), handler);
  }

  @Override
  public Future<Void> end(String chunk, String enc) {
    Promise<Void> promise = Promise.promise();
    end(chunk, enc, promise);
    return promise.future();
  }

  @Override
  public void end(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
    Objects.requireNonNull(enc, "no null encoding accepted");
    end(Buffer.buffer(chunk, enc), handler);
  }

  @Override
  public Future<Void> end(Buffer chunk) {
    Promise<Void> promise = Promise.promise();
    write(chunk.getByteBuf(), true, promise);
    return promise.future();
  }

  @Override
  public void end(Buffer chunk, Handler<AsyncResult<Void>> handler) {
    write(chunk.getByteBuf(), true, handler);
  }

  @Override
  public Future<Void> end() {
    Promise<Void> promise = Promise.promise();
    end(promise);
    return promise.future();
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    write(null, true, handler);
  }

  @Override
  public Future<Void> write(Buffer chunk) {
    Promise<Void> promise = Promise.promise();
    write(chunk, promise);
    return promise.future();
  }

  @Override
  public void write(Buffer chunk, Handler<AsyncResult<Void>> handler) {
    ByteBuf buf = chunk.getByteBuf();
    write(buf, false, handler);
  }

  @Override
  public Future<Void> write(String chunk) {
    Promise<Void> promise = Promise.promise();
    write(chunk, promise);
    return promise.future();
  }

  @Override
  public void write(String chunk, Handler<AsyncResult<Void>> handler) {
    write(Buffer.buffer(chunk).getByteBuf(), false, handler);
  }

  @Override
  public Future<Void> write(String chunk, String enc) {
    Promise<Void> promise = Promise.promise();
    write(chunk, enc, promise);
    return promise.future();
  }

  @Override
  public void write(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
    Objects.requireNonNull(enc, "no null encoding accepted");
    write(Buffer.buffer(chunk, enc).getByteBuf(), false, handler);
  }

  // 检查是否需要设置Content-Length
  private boolean requiresContentLength() {
    return !chunked && (headers == null || !headers.contains(CONTENT_LENGTH));
  }

  // 写入buffer并发送
  private void write(ByteBuf buff, boolean end, Handler<AsyncResult<Void>> completionHandler) {
    if (buff == null && !end) {
      return;
    }
    HttpClientStream s;
    synchronized (this) {
      // 请求已结束
      if (ended) {
        completionHandler.handle(Future.failedFuture(new IllegalStateException("Request already complete")));
        return;
      }
      // 连接server前需要先设置handler
      checkResponseHandler();
      // 是否要完成该请求
      if (end) {
        // 请求头是否需要设置Content-Length
        if (buff != null && requiresContentLength()) {
          headers().set(CONTENT_LENGTH, String.valueOf(buff.readableBytes()));
        }
      } else if (requiresContentLength()) {
        throw new IllegalStateException("You must set the Content-Length header to be the total size of the message "
          + "body BEFORE sending any data if you are not using HTTP chunked encoding.");
      }
      ended |= end;
      if (stream == null) {
        if (buff != null) {
          // 将写入数据暂存到pendingChunks，如果已存在则合并
          if (pendingChunks == null) {
            // 暂存请求数据
            pendingChunks = buff;
          } else {
            // 将pendingChunks转为CompositeByteBuf，继续写入buffer中的内容
            CompositeByteBuf pending;
            if (pendingChunks instanceof CompositeByteBuf) {
              pending = (CompositeByteBuf) pendingChunks;
            } else {
              pending = Unpooled.compositeBuffer();
              pending.addComponent(true, pendingChunks);
              pendingChunks = pending;
            }
            // 追加请求data
            pending.addComponent(true, buff);
          }
          // 每次调用write时传入的handler
          if (completionHandler != null) {
            if (pendingHandlers == null) {
              pendingHandlers = new ArrayList<>();
            }
            pendingHandlers.add(completionHandler);
          }
        }
        // 发送请求,会创建stream
        connect(null);
        return;
      }
      s = stream;
    }
    // 发送报文
    s.writeBuffer(buff, end, completionHandler);
    if (end) {
      // 更新当前发送数据的stream，回收连接，通知回调
      s.endRequest();
      tryComplete();
    }
  }

  private void checkEnded() {
    if (ended) {
      throw new IllegalStateException("Request already complete");
    }
  }

  // 连接server前需要先设置handler
  private void checkResponseHandler() {
    if (stream == null && !connecting && responsePromise.future().getHandler() == null) {
      throw new IllegalStateException("You must set a response handler before connecting to the server");
    }
  }

  synchronized Handler<HttpClientRequest> pushHandler() {
    return pushHandler;
  }

  @Override
  public synchronized HttpClientRequest setStreamPriority(StreamPriority priority) {
    synchronized (this) {
      if (stream != null) {
        stream.updatePriority(priority);
      } else {
        this.priority = priority;
      }
    }
    return this;
  }

  @Override
  public synchronized StreamPriority getStreamPriority() {
    HttpClientStream s = stream;
    return s != null ? s.priority() : priority;
  }
}
