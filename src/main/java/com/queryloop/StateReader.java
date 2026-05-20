package com.queryloop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.Message;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class StateReader {

    private final ChatMemory chatMemory;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final StateBus stateBus;

    /** Phase 3: 本地降级缓存（Redis 不可用时回退） */
    private final Map<String, SessionState> localFallback = new ConcurrentHashMap<>();

    private static final int DEFAULT_HISTORY_SIZE = 20;
    private static final String SESSION_KEY_PREFIX = "session:";

    public StateReader(ChatMemory chatMemory, StringRedisTemplate redisTemplate,
                       StateBus stateBus) {
        this.chatMemory = chatMemory;
        this.redisTemplate = redisTemplate;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.stateBus = stateBus;
    }

    public void readState(QueryLoopContext ctx, String traceId) {
        String conversationId = ctx.getUserId();
        log.info("[QL][{}] ▶ 1/5 StateReader conversationId={}", traceId, conversationId);

        // ── 读取消息历史 ──
        try {
            List<Message> history = chatMemory.get(conversationId, DEFAULT_HISTORY_SIZE);
            ctx.setChatHistory(history);
            if (history.isEmpty()) {
                log.info("[QL][{}]    ↳ 消息历史：空", traceId);
            } else {
                String summary = history.stream()
                        .collect(Collectors.groupingBy(
                                m -> m.getMessageType().getValue(),
                                Collectors.counting()))
                        .entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(", "));
                log.info("[QL][{}]    ↳ 消息历史：size={} breakdown=[{}]", traceId, history.size(), summary);
            }
        } catch (Exception e) {
            log.warn("[QL][{}]    ↳ ChatMemory 读取异常（Redis 可能不可用），使用空历史", traceId);
            ctx.setChatHistory(List.of());
        }

        // ── 读取结构化会话状态（Redis 优先 → 本地兜底） ──
        SessionState sessionState = readSessionState(conversationId, traceId);
        ctx.setSessionState(sessionState);
    }

    /**
     * Phase 3: 双层读取 —— Redis 主存储 + 本地内存兜底
     */
    private SessionState readSessionState(String conversationId, String traceId) {
        // Layer 1: Redis
        String sessionKey = SESSION_KEY_PREFIX + conversationId;
        try {
            String json = redisTemplate.opsForValue().get(sessionKey);
            if (json != null && !json.isEmpty()) {
                SessionState state = objectMapper.readValue(json, SessionState.class);
                log.info("[QL][{}]    ↳ 会话状态：Redis 命中 lastIntent={} activeContext={} totalMsg={}",
                        traceId, state.getLastIntent(), state.getActiveContext(), state.getTotalMessages());

                // Phase 3: 写穿到本地缓存（加速后续读取）
                localFallback.put(conversationId, state);

                // Phase 1: 日志输出反馈统计
                if (!state.getFeedbackByIntent().isEmpty()) {
                    log.info("[QL][{}]    ↳ 反馈统计: {}", traceId, state.getFeedbackByIntent().entrySet().stream()
                            .map(e -> e.getKey() + "=" + String.format("adopt=%.0f%%", e.getValue().getAdoptionRate() * 100))
                            .collect(Collectors.joining(", ")));
                }
                return state;
            }
        } catch (Exception e) {
            log.warn("[QL][{}]    ↳ Redis 读取异常，降级到本地缓存 err={}", traceId, e.toString());
        }

        // Layer 2: 本地内存兜底
        SessionState local = localFallback.get(conversationId);
        if (local != null) {
            log.info("[QL][{}]    ↳ 会话状态：本地缓存命中（Redis 降级）", traceId);
            return local;
        }

        // Layer 3: 全新会话
        log.info("[QL][{}]    ↳ 会话状态：空（新会话）", traceId);
        SessionState fresh = new SessionState().setUserId(conversationId);
        localFallback.put(conversationId, fresh);
        return fresh;
    }

    /**
     * Phase 3: 暴露本地缓存用于故障后的状态恢复
     */
    public Map<String, SessionState> getLocalFallbackView() {
        return Map.copyOf(localFallback);
    }

    /**
     * Phase 1: 直接读取某用户的 SessionState（供反馈/统计等非管线场景使用）
     */
    public SessionState getSessionState(String userId) {
        return readSessionState(userId, "feedback");
    }

    // ═══════════════════════════════════════════════
    // StateBus 集成：反应式状态订阅
    // ═══════════════════════════════════════════════

    /**
     * 通过 StateBus 订阅用户状态变更。
     *
     * <p>与直接调用 {@link #getSessionState} 的区别：
     * <ul>
     *   <li>本方法返回最新的总线快照（可能比 Redis 更新）</li>
     *   <li>支持回调式订阅，当其他 Agent 修改状态时自动收到通知</li>
     * </ul>
     *
     * @param userId   用户 ID
     * @param callback 状态变更回调
     * @return 订阅句柄
     */
    public StateBus.Subscription subscribe(String userId, java.util.function.Consumer<StateEvent> callback) {
        return stateBus.subscribe(userId, callback);
    }

    /**
     * 从 StateBus 获取最新快照（可能比 Redis 更新，因尚未持久化）。
     */
    public SessionState getBusSnapshot(String userId) {
        SessionState fromBus = stateBus.getSnapshot(userId);
        if (fromBus != null) return fromBus;
        // 回退到本地缓存
        return localFallback.get(userId);
    }

    /**
     * 以 Reactor Flux 观察状态变更，供响应式链路使用。
     */
    public reactor.core.publisher.Flux<StateEvent> observe(String userId) {
        return stateBus.observe(userId);
    }
}
