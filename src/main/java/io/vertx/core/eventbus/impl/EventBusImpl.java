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

package io.vertx.core.eventbus.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Closeable;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.utils.ConcurrentCyclicSequence;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.metrics.EventBusMetrics;
import io.vertx.core.spi.metrics.MetricsProvider;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * A local event bus implementation
 *
 * @author <a href="http://tfox.org">Tim Fox</a>                                                                                        T
 */
public class EventBusImpl implements EventBus, MetricsProvider {

  private static final Logger log = LoggerFactory.getLogger(EventBusImpl.class);

  private final List<Handler<DeliveryContext>> sendInterceptors = new CopyOnWriteArrayList<>();
  private final List<Handler<DeliveryContext>> receiveInterceptors = new CopyOnWriteArrayList<>();
  private final AtomicLong replySequence = new AtomicLong(0);
  protected final VertxInternal vertx;
  protected final EventBusMetrics metrics;
  // 保存messageConsumer对应的handler
  protected final ConcurrentMap<String, ConcurrentCyclicSequence<HandlerHolder>> handlerMap = new ConcurrentHashMap<>();
  protected final CodecManager codecManager = new CodecManager();
  protected volatile boolean started;
  private final ContextInternal sendNoContext;

  public EventBusImpl(VertxInternal vertx) {
    VertxMetrics metrics = vertx.metricsSPI();
    this.vertx = vertx;
    this.metrics = metrics != null ? metrics.createEventBusMetrics() : null;
    this.sendNoContext = vertx.getOrCreateContext();
  }

  @Override
  public <T> EventBus addOutboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    sendInterceptors.add((Handler) interceptor);
    return this;
  }

  @Override
  public <T> EventBus addInboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    receiveInterceptors.add((Handler)interceptor);
    return this;
  }

  @Override
  public <T> EventBus removeOutboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    sendInterceptors.remove(interceptor);
    return this;
  }

  Iterator<Handler<DeliveryContext>> receiveInterceptors() {
    return receiveInterceptors.iterator();
  }

  @Override
  public <T> EventBus removeInboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    receiveInterceptors.remove(interceptor);
    return this;
  }

  // 设置标志位并调用completionHandler
  public synchronized void start(Handler<AsyncResult<Void>> completionHandler) {
    if (started) {
      throw new IllegalStateException("Already started");
    }
    started = true;
    completionHandler.handle(Future.succeededFuture());
  }

  @Override
  public EventBus send(String address, Object message) {
    return request(address, message, new DeliveryOptions(), null);
  }

  @Override
  public <T> EventBus request(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
    return request(address, message, new DeliveryOptions(), replyHandler);
  }

  @Override
  public EventBus send(String address, Object message, DeliveryOptions options) {
    return request(address, message, options, null);
  }

  @Override
  public <T> EventBus request(String address, Object message, DeliveryOptions options, Handler<AsyncResult<Message<T>>> replyHandler) {
    sendOrPubInternal(createMessage(true, true, address, options.getHeaders(), message, options.getCodecName(), null), options, replyHandler);
    return this;
  }

  @Override
  public <T> MessageProducer<T> sender(String address) {
    Objects.requireNonNull(address, "address");
    return new MessageProducerImpl<>(vertx, address, true, new DeliveryOptions());
  }

  @Override
  public <T> MessageProducer<T> sender(String address, DeliveryOptions options) {
    Objects.requireNonNull(address, "address");
    Objects.requireNonNull(options, "options");
    return new MessageProducerImpl<>(vertx, address, true, options);
  }

  @Override
  public <T> MessageProducer<T> publisher(String address) {
    Objects.requireNonNull(address, "address");
    return new MessageProducerImpl<>(vertx, address, false, new DeliveryOptions());
  }

  @Override
  public <T> MessageProducer<T> publisher(String address, DeliveryOptions options) {
    Objects.requireNonNull(address, "address");
    Objects.requireNonNull(options, "options");
    return new MessageProducerImpl<>(vertx, address, false, options);
  }

  @Override
  public EventBus publish(String address, Object message) {
    return publish(address, message, new DeliveryOptions());
  }

  @Override
  public EventBus publish(String address, Object message, DeliveryOptions options) {
    sendOrPubInternal(createMessage(false, true, address, options.getHeaders(), message, options.getCodecName(), null), options, null);
    return this;
  }

  @Override
  public <T> MessageConsumer<T> consumer(String address) {
    checkStarted();
    Objects.requireNonNull(address, "address");
    return new HandlerRegistration<>(vertx, metrics, this, address, null, false, false);
  }

  @Override
  public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
    Objects.requireNonNull(handler, "handler");
    // 获取对应的MessageConsumer
    MessageConsumer<T> consumer = consumer(address);
    // 绑定handler
    consumer.handler(handler);
    return consumer;
  }

  @Override
  public <T> MessageConsumer<T> localConsumer(String address) {
    checkStarted();
    Objects.requireNonNull(address, "address");
    return new HandlerRegistration<>(vertx, metrics, this, address, null, true, false);
  }

  @Override
  public <T> MessageConsumer<T> localConsumer(String address, Handler<Message<T>> handler) {
    Objects.requireNonNull(handler, "handler");
    MessageConsumer<T> consumer = localConsumer(address);
    consumer.handler(handler);
    return consumer;
  }

  @Override
  public EventBus registerCodec(MessageCodec codec) {
    codecManager.registerCodec(codec);
    return this;
  }

  @Override
  public EventBus unregisterCodec(String name) {
    codecManager.unregisterCodec(name);
    return this;
  }

  @Override
  public <T> EventBus registerDefaultCodec(Class<T> clazz, MessageCodec<T, ?> codec) {
    codecManager.registerDefaultCodec(clazz, codec);
    return this;
  }

  @Override
  public EventBus unregisterDefaultCodec(Class clazz) {
    codecManager.unregisterDefaultCodec(clazz);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    checkStarted();
    unregisterAll();
    if (metrics != null) {
      metrics.close();
    }
    if (completionHandler != null) {
      vertx.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
    }
  }

  @Override
  public boolean isMetricsEnabled() {
    return metrics != null;
  }

  @Override
  public EventBusMetrics<?> getMetrics() {
    return metrics;
  }

  public MessageImpl createMessage(boolean send, boolean src, String address, MultiMap headers, Object body, String codecName, Handler<AsyncResult<Void>> writeHandler) {
    Objects.requireNonNull(address, "no null address accepted");
    // 获取解码组件
    MessageCodec codec = codecManager.lookupCodec(body, codecName);
    @SuppressWarnings("unchecked")
    MessageImpl msg = new MessageImpl(address, null, headers, body, codec, send, src, this, writeHandler);
    return msg;
  }

  protected <T> HandlerHolder<T> addRegistration(String address, HandlerRegistration<T> registration,
                                     boolean replyHandler, boolean localOnly) {
    Objects.requireNonNull(registration.getHandler(), "handler");
    // 将handler封装为HandlerHolder，并注册到eventBus
    LocalRegistrationResult<T> result = addLocalRegistration(address, registration, replyHandler, localOnly);
    addRegistration(result.newAddress, address, replyHandler, localOnly, registration::setResult);
    return result.holder;
  }

  protected <T> void addRegistration(boolean newAddress, String address,
                                     boolean replyHandler, boolean localOnly,
                                     Handler<AsyncResult<Void>> completionHandler) {
    completionHandler.handle(Future.succeededFuture());
  }

  private static class LocalRegistrationResult<T> {
    final HandlerHolder<T> holder;
    final boolean newAddress;
    LocalRegistrationResult(HandlerHolder<T> holder, boolean newAddress) {
      this.holder = holder;
      this.newAddress = newAddress;
    }
  }

  private <T> LocalRegistrationResult<T> addLocalRegistration(String address, HandlerRegistration<T> registration,
                                                           boolean replyHandler, boolean localOnly) {
    Objects.requireNonNull(address, "address");
    // 获取当前线程下对应的Vert.x Context，如果获取不到则表明当前不在Verticle中(即Embedded)
    Context context = Vertx.currentContext();
    boolean hasContext = context != null;
    if (!hasContext) {
      // Embedded
      context = vertx.getOrCreateContext();
    }
    registration.setHandlerContext(context);

    // 使用HandlerHolder封装
    HandlerHolder<T> holder = new HandlerHolder<>(registration, replyHandler, localOnly, context);

    // 注册到eventBus.handlerMap
    ConcurrentCyclicSequence<HandlerHolder> handlers = new ConcurrentCyclicSequence<HandlerHolder>().add(holder);
    ConcurrentCyclicSequence<HandlerHolder> actualHandlers = handlerMap.merge(
      address,
      handlers,
      (old, prev) -> old.add(prev.first()));

    // 用于取消注册
    if (hasContext) {
      HandlerEntry entry = new HandlerEntry<>(address, registration);
      context.addCloseHook(entry);
    }

    boolean newAddress = handlers == actualHandlers;
    return new LocalRegistrationResult<>(holder, newAddress);
  }

  protected <T> void removeRegistration(HandlerHolder<T> holder, Handler<AsyncResult<Void>> completionHandler) {
    boolean last = removeLocalRegistration(holder);
    removeRegistration(last ? holder : null, holder.getHandler().address(), completionHandler);
  }

  protected <T> void removeRegistration(HandlerHolder<T> handlerHolder, String address,
                                        Handler<AsyncResult<Void>> completionHandler) {
    callCompletionHandlerAsync(completionHandler);
  }

  private <T> boolean removeLocalRegistration(HandlerHolder<T> holder) {
    String address = holder.getHandler().address();
    boolean last = handlerMap.compute(address, (key, val) -> {
      if (val == null) {
        return null;
      }
      ConcurrentCyclicSequence<HandlerHolder> next = val.remove(holder);
      return next.size() == 0 ? null : next;
    }) == null;
    if (holder.setRemoved()) {
      holder.getContext().removeCloseHook(new HandlerEntry<>(address, holder.getHandler()));
    }
    return last;
  }

  protected <T> void sendReply(MessageImpl replyMessage, MessageImpl replierMessage, DeliveryOptions options,
                               Handler<AsyncResult<Message<T>>> replyHandler) {
    if (replyMessage.address() == null) {
      throw new IllegalStateException("address not specified");
    } else {
      ContextInternal ctx = vertx.getOrCreateContext();
      if (ctx == null) {
        // Guarantees the order when there is no current context in clustered mode
        ctx = sendNoContext;
      }
      ReplyHandler<T> handler = createReplyHandler(replyMessage, replierMessage.src, options, replyHandler);
      new OutboundDeliveryContext<>(ctx, replyMessage, options, handler, replierMessage).next();
    }
  }

  // 发送响应
  protected <T> void sendReply(OutboundDeliveryContext<T> sendContext, MessageImpl replierMessage) {
    sendOrPub(sendContext);
  }

  // 发送请求
  protected <T> void sendOrPub(OutboundDeliveryContext<T> sendContext) {
    sendLocally(sendContext);
  }

  // 记录消息发送
  protected final Object messageSent(OutboundDeliveryContext<?> sendContext, boolean local, boolean remote) {
    MessageImpl msg = sendContext.message;
    // 统计
    if (metrics != null) {
      MessageImpl message = msg;
      metrics.messageSent(message.address(), !message.send, local, remote);
    }
    VertxTracer tracer = sendContext.ctx.tracer();
    if (tracer != null && msg.src) {
      BiConsumer<String, String> biConsumer = (String key, String val) -> msg.headers().set(key, val);
      return tracer.sendRequest(sendContext.ctx, msg, msg.send ? "send" : "publish", biConsumer, MessageTagExtractor.INSTANCE);
    } else {
      return null;
    }
  }

  protected void callCompletionHandlerAsync(Handler<AsyncResult<Void>> completionHandler) {
    if (completionHandler != null) {
      vertx.runOnContext(v -> {
        completionHandler.handle(Future.succeededFuture());
      });
    }
  }

  private <T> void sendLocally(OutboundDeliveryContext<T> sendContext) {
    // 记录message发送信息
    Object trace = messageSent(sendContext, true, false);
    // 发送message
    ReplyException failure = deliverMessageLocally(sendContext.message);
    if (failure != null) {
      // no handlers
      VertxTracer tracer = sendContext.ctx.tracer();
      if (sendContext.replyHandler != null) {
        sendContext.replyHandler.trace = trace;
        sendContext.replyHandler.fail(failure);
      } else {
        if (tracer != null && sendContext.message.src) {
          tracer.receiveResponse(sendContext.ctx, null, trace, failure, TagExtractor.empty());
        }
      }
    } else {
      failure = null;
      VertxTracer tracer = sendContext.ctx.tracer();
      if (tracer != null && sendContext.message.src) {
        if (sendContext.replyHandler == null) {
          tracer.receiveResponse(sendContext.ctx, null, trace, null, TagExtractor.empty());
        } else {
          sendContext.replyHandler.trace = trace;
        }
      }
    }
  }

  protected boolean isMessageLocal(MessageImpl msg) {
    return true;
  }

  // 本地分发message，调用对应的handler
  protected ReplyException deliverMessageLocally(MessageImpl msg) {
    ConcurrentCyclicSequence<HandlerHolder> handlers = handlerMap.get(msg.address());
    if (handlers != null) {
      // send方法，使用点对点通信
      if (msg.isSend()) {
        // 选择一个handler
        HandlerHolder holder = handlers.next();
        // 记录消息接收信息
        if (metrics != null) {
          metrics.messageReceived(msg.address(), !msg.isSend(), isMessageLocal(msg), holder != null ? 1 : 0);
        }
        if (holder != null) {
          // 将message分发到handler进行处理
          deliverToHandler(msg, holder);
          // 通知writeHandler
          Handler<AsyncResult<Void>> handler = msg.writeHandler;
          if (handler != null) {
            handler.handle(Future.succeededFuture());
          }
        } else {
          // RACY issue !!!!!
        }
      } else {
        // Publish 广播
        if (metrics != null) {
          metrics.messageReceived(msg.address(), !msg.isSend(), isMessageLocal(msg), handlers.size());
        }
        for (HandlerHolder holder: handlers) {
          deliverToHandler(msg, holder);
        }
        Handler<AsyncResult<Void>> handler = msg.writeHandler;
        if (handler != null) {
          handler.handle(Future.succeededFuture());
        }
      }
      return null;
    } else {
      // handler为空，返回失败
      if (metrics != null) {
        metrics.messageReceived(msg.address(), !msg.isSend(), isMessageLocal(msg), 0);
      }
      ReplyException failure = new ReplyException(ReplyFailure.NO_HANDLERS, "No handlers for address " + msg.address);
      Handler<AsyncResult<Void>> handler = msg.writeHandler;
      if (handler != null) {
        handler.handle(Future.failedFuture(failure));
      }
      return failure;
    }
  }

  protected void checkStarted() {
    if (!started) {
      throw new IllegalStateException("Event Bus is not started");
    }
  }

  // 创建唯一标识
  protected String generateReplyAddress() {
    return "__vertx.reply." + Long.toString(replySequence.incrementAndGet());
  }

  // 创建ReplyHandler(复制处理回复)
  private <T> ReplyHandler<T> createReplyHandler(MessageImpl message,
                                                 boolean src,
                                                 DeliveryOptions options,
                                                 Handler<AsyncResult<Message<T>>> replyHandler) {
    if (replyHandler != null) {
      long timeout = options.getSendTimeout();
      // 创建唯一标识
      String replyAddress = generateReplyAddress();
      message.setReplyAddress(replyAddress);
      HandlerRegistration<T> registration = new HandlerRegistration<>(vertx, metrics, this, replyAddress, message.address, true, src);
      ReplyHandler<T> handler = new ReplyHandler<>(registration, timeout);
      // 设置回复处理方法
      handler.result.future().setHandler(replyHandler);
      registration.handler(handler);
      return handler;
    } else {
      return null;
    }
  }

  public class ReplyHandler<T> implements Handler<Message<T>> {

    final Promise<Message<T>> result;
    final HandlerRegistration<T> registration;
    final long timeoutID;
    public Object trace;

    ReplyHandler(HandlerRegistration<T> registration, long timeout) {
      this.result = Promise.promise();
      this.registration = registration;
      // 添加超时任务
      this.timeoutID = vertx.setTimer(timeout, id -> {
        fail(new ReplyException(ReplyFailure.TIMEOUT, "Timed out after waiting " + timeout + "(ms) for a reply. address: " + registration.address + ", repliedAddress: " + registration.repliedAddress));
      });
    }

    private void trace(Object reply, Throwable failure) {
      ContextInternal ctx = registration.handlerContext();
      VertxTracer tracer = ctx.tracer();
      if (tracer != null && registration.src) {
        tracer.receiveResponse(ctx, reply, trace, failure, TagExtractor.empty());
      }
    }

    // 失败
    void fail(ReplyException failure) {
      // 取消注册
      registration.unregister();
      // 记录统计
      if (metrics != null) {
        metrics.replyFailure(registration.repliedAddress, failure.failureType());
      }
      trace(null, failure);
      // promise失败
      result.tryFail(failure);
    }

    @Override
    public void handle(Message<T> reply) {
      // 取消超时任务
      vertx.cancelTimer(timeoutID);
      if (reply.body() instanceof ReplyException) {
        // 调用失败
        // This is kind of clunky - but hey-ho
        fail((ReplyException) reply.body());
      } else {
        // 通知下游
        trace(reply, null);
        result.complete(reply);
      }
    }
  }

  public <T> void sendOrPubInternal(MessageImpl message, DeliveryOptions options,
                                    Handler<AsyncResult<Message<T>>> replyHandler) {
    // 检查是否已经启动
    checkStarted();
    // 负责处理回复的handler
    ReplyHandler<T> handler = createReplyHandler(message, true, options, replyHandler);
    ContextInternal ctx = vertx.getContext();
    if (ctx == null) {
      // Guarantees the order when there is no current context in clustered mode
      ctx = sendNoContext;
    }
    // 发送上下文
    OutboundDeliveryContext<T> sendContext = new OutboundDeliveryContext<>(ctx, message, options, handler);
    sendContext.next();
  }

  protected class OutboundDeliveryContext<T> implements DeliveryContext<T> {

    public final ContextInternal ctx;
    public final MessageImpl message;
    public final DeliveryOptions options;
    // sendInterceptors.iterator
    public final Iterator<Handler<DeliveryContext>> iter;
    public final ReplyHandler<T> replyHandler;
    private final MessageImpl replierMessage;

    private OutboundDeliveryContext(ContextInternal ctx, MessageImpl message, DeliveryOptions options, ReplyHandler<T> replyHandler) {
      this(ctx, message, options, replyHandler, null);
    }

    private OutboundDeliveryContext(ContextInternal ctx, MessageImpl message, DeliveryOptions options, ReplyHandler<T> replyHandler, MessageImpl replierMessage) {
      this.ctx = ctx;
      this.message = message;
      this.options = options;
      this.iter = sendInterceptors.iterator();
      this.replierMessage = replierMessage;
      this.replyHandler = replyHandler;
    }

    @Override
    public Message<T> message() {
      return message;
    }

    // 调用消息拦截器，并发送message
    @Override
    public void next() {
      // 调用sendInterceptors
      if (iter.hasNext()) {
        Handler<DeliveryContext> handler = iter.next();
        try {
          if (handler != null) {
            handler.handle(this);
          } else {
            next();
          }
        } catch (Throwable t) {
          log.error("Failure in interceptor", t);
        }
      } else {
        if (replierMessage == null) {
          // 发送请求
          sendOrPub(this);
        } else {
          // 发送响应
          sendReply(this, replierMessage);
        }
      }
    }

    // 是否是点对点模式
    @Override
    public boolean send() {
      return message.isSend();
    }

    @Override
    public Object body() {
      return message.sentBody;
    }
  }

  private void unregisterAll() {
    // Unregister all handlers explicitly - don't rely on context hooks
    for (ConcurrentCyclicSequence<HandlerHolder> handlers: handlerMap.values()) {
      for (HandlerHolder holder: handlers) {
        holder.getHandler().unregister();
      }
    }
  }

  private <T> void deliverToHandler(MessageImpl msg, HandlerHolder<T> holder) {
    // Each handler gets a fresh copy
    MessageImpl copied = msg.copyBeforeReceive(holder.getHandler().src);

    if (metrics != null) {
      metrics.scheduleMessage(holder.getHandler().getMetric(), msg.isLocal());
    }

    // 在Vert.x Context中处理message
    holder.getContext().runOnContext((v) -> {
      // Need to check handler is still there - the handler might have been removed after the message were sent but
      // before it was received
      try {
        // 检查handler是否已经被移除
        if (!holder.isRemoved()) {
          // HandlerRegistration执行消息处理逻辑
          holder.getHandler().handle(copied);
        }
      } finally {
        // 注销回复过的ReplyHandler
        if (holder.isReplyHandler()) {
          holder.getHandler().unregister();
        }
      }
    });
  }

  public class HandlerEntry<T> implements Closeable {
    final String address;
    final HandlerRegistration<T> handler;

    public HandlerEntry(String address, HandlerRegistration<T> handler) {
      this.address = address;
      this.handler = handler;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) return false;
      if (this == o) return true;
      if (getClass() != o.getClass()) return false;
      HandlerEntry entry = (HandlerEntry) o;
      if (!address.equals(entry.address)) return false;
      if (!handler.equals(entry.handler)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      int result = address != null ? address.hashCode() : 0;
      result = 31 * result + (handler != null ? handler.hashCode() : 0);
      return result;
    }

    // Called by context on undeploy
    public void close(Handler<AsyncResult<Void>> completionHandler) {
      handler.unregister(completionHandler);
    }

  }

  @Override
  protected void finalize() throws Throwable {
    // Make sure this gets cleaned up if there are no more references to it
    // so as not to leave connections and resources dangling until the system is shutdown
    // which could make the JVM run out of file handles.
    close(ar -> {});
    super.finalize();
  }

}

