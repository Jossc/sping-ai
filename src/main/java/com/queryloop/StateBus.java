package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 集中式状态总线 —— 所有 SessionState 变更都经过此处，按序广播。
 *
 * <h3>解决的问题</h3>
 * <pre>
 *   多 Agent 并发时：
 *     Agent A 写 → Agent B 读 → Agent C 也读
 *                       ↑
 *                 B 和 C 可能读到不同的状态
 *
 *   解决方案：
 *     ┌─────────┐    ┌─────────┐    ┌─────────┐
 *     │ Agent A │───▶│  State  │◀───│ Agent B │
 *     └─────────┘    │  Bus    │    └─────────┘
 *                    │         │
 *                    │ State1  │◀─── Agent C (订阅)
 *                    │ State2  │
 *                    └────┬────┘
 *                          │
 *                     所有状态变更
 *                     都经过总线
 *                     按顺序广播
 * </pre>
 *
 * <h3>使用方式</h3>
 * <ul>
 *   <li><b>发布</b>：{@link #publish(String, SessionState, SessionState, String)} — 状态变更时调用</li>
 *   <li><b>订阅</b>：{@link #subscribe(String, Consumer)} — 接收该用户的所有后续变更</li>
 *   <li><b>快照</b>：{@link #getSnapshot(String)} — 获取最新一致状态</li>
 *   <li><b>Flux 订阅</b>：{@link #observe(String)} — 返回 Flux<StateEvent> 供 Reactor 链路使用</li>
 * </ul>
 */
@Slf4j
@Component
public class StateBus {

    /**
     * 每用户一个 Sinks.Many（replay(1) = 缓存最新一条，新订阅者立即收到当前状态）。
     * 使用 Sinks.many().multicast().onBackpressureBuffer() 保证有序广播。
     */
    private final Map<String, UserTopic> topics = new ConcurrentHashMap<>();

    /**
     * 每用户的事件序号（单调递增）
     */
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    /**
     * 发布状态变更。
     *
     * @param userId   用户 ID
     * @param previous 变更前的状态（可为 null，表示首次创建）
     * @param current  变更后的状态（必须非 null）
     * @param source   变更来源标识
     * @return 发布的事件
     */
    public StateEvent publish(String userId, SessionState previous, SessionState current, String source) {
        if (current == null) {
            throw new IllegalArgumentException("current 状态不能为 null");
        }

        AtomicLong seq = sequences.computeIfAbsent(userId, k -> new AtomicLong(0));
        long sequenceNumber = seq.incrementAndGet();

        StateEvent event = new StateEvent(userId, previous, current, source, sequenceNumber);

        UserTopic topic = topics.computeIfAbsent(userId, k -> new UserTopic(userId));
        Sinks.EmitResult result = topic.sink.tryEmitNext(event);

        if (result.isFailure()) {
            log.warn("[StateBus] 事件发布失败 userId={} seq={} result={}", userId, sequenceNumber, result);
        } else {
            log.debug("[StateBus] 事件已发布 userId={} seq={} intent={} ctx={} src={}",
                    userId, sequenceNumber, current.getLastIntent(), current.getActiveContext(), source);
        }

        // 更新缓存快照
        topic.latestState = current;

        return event;
    }

    /**
     * 订阅指定用户的状态变更。
     *
     * <p>回调在 Sinks 的 emitting 线程中执行，应轻量且不阻塞。
     * 如需异步处理，在回调内部提交到线程池。</p>
     *
     * @param userId   用户 ID
     * @param callback 每次状态变更时调用
     * @return 用于取消订阅的句柄
     */
    public Subscription subscribe(String userId, Consumer<StateEvent> callback) {
        UserTopic topic = topics.computeIfAbsent(userId, k -> new UserTopic(userId));
        return topic.addSubscriber(callback);
    }

    /**
     * 以 Reactor Flux 的方式观察指定用户的状态变更。
     *
     * <p>返回的 Flux 是 hot stream：订阅后才收到事件，不会重放历史。
     * 如需获取当前状态，先调用 {@link #getSnapshot}。</p>
     */
    public Flux<StateEvent> observe(String userId) {
        UserTopic topic = topics.computeIfAbsent(userId, k -> new UserTopic(userId));
        return topic.sink.asFlux();
    }

    /**
     * 获取指定用户的最新一致状态快照。
     *
     * @return 最新 SessionState，若未曾写入则返回 null
     */
    public SessionState getSnapshot(String userId) {
        UserTopic topic = topics.get(userId);
        return topic != null ? topic.latestState : null;
    }

    /**
     * 获取事件序号（可用于判断状态是否已被更新）。
     */
    public long getSequence(String userId) {
        AtomicLong seq = sequences.get(userId);
        return seq != null ? seq.get() : 0;
    }

    /**
     * 取消指定用户的所有订阅（用户会话结束时清理）。
     */
    public void close(String userId) {
        UserTopic topic = topics.remove(userId);
        if (topic != null) {
            topic.dispose();
            sequences.remove(userId);
            log.debug("[StateBus] 用户主题已关闭 userId={}", userId);
        }
    }

    /**
     * 当前活跃的用户数。
     */
    public int activeUsers() {
        return topics.size();
    }

    // ═══════════════════════════════════════════════
    // 内部：用户主题
    // ═══════════════════════════════════════════════

    private static class UserTopic {
        final String userId;
        final Sinks.Many<StateEvent> sink;
        volatile SessionState latestState;
        // 使用 CopyOnWriteArrayList 或并发安全的订阅者管理
        private final Map<String, Consumer<StateEvent>> subscribers = new ConcurrentHashMap<>();

        UserTopic(String userId) {
            this.userId = userId;
            // multicast: 多个订阅者 + backpressure buffer
            this.sink = Sinks.many().multicast().onBackpressureBuffer(64, false);

            // 内部订阅：将 Sinks 事件分发给回调订阅者
            this.sink.asFlux().subscribe(event -> {
                for (Consumer<StateEvent> cb : subscribers.values()) {
                    try {
                        cb.accept(event);
                    } catch (Exception e) {
                        log.warn("[StateBus] 订阅者回调异常 userId={} err={}", userId, e.toString());
                    }
                }
            });
        }

        Subscription addSubscriber(Consumer<StateEvent> callback) {
            String id = java.util.UUID.randomUUID().toString().substring(0, 8);
            subscribers.put(id, callback);
            return () -> subscribers.remove(id);
        }

        void dispose() {
            subscribers.clear();
            sink.tryEmitComplete();
        }
    }

    /**
     * 订阅句柄，调用 {@link #cancel()} 取消订阅。
     */
    @FunctionalInterface
    public interface Subscription {
        void cancel();
    }
}
