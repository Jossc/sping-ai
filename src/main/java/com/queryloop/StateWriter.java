package com.queryloop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class StateWriter {

    private final ChatMemory chatMemory;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final StateBus stateBus;

    /** Phase 3: 本地降级缓存（Redis 不可用时回退） */
    private final Map<String, SessionState> localFallback = new ConcurrentHashMap<>();

    private static final String SESSION_KEY_PREFIX = "session:";
    private static final Duration SESSION_TTL = Duration.ofDays(7);

    public StateWriter(ChatMemory chatMemory, StringRedisTemplate redisTemplate,
                       StateBus stateBus) {
        this.chatMemory = chatMemory;
        this.redisTemplate = redisTemplate;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.stateBus = stateBus;
    }

    public void writeState(QueryLoopContext ctx, String traceId) {
        String conversationId = ctx.getUserId();
        String response = ctx.getResponse();

        log.info("[QL][{}] ▶ 4/5 StateWriter conversationId={} responseLen={}",
                traceId, conversationId, response != null ? response.length() : 0);

        if (response == null || response.isBlank()) {
            log.warn("[QL][{}]    ↳ 空响应，跳过持久化", traceId);
            return;
        }

        // ── 1. 写入消息到 ChatMemory ──
        try {
            chatMemory.add(conversationId, List.of(
                    new UserMessage(ctx.getCleanedInput()),
                    new AssistantMessage(response)
            ));
        } catch (Exception e) {
            log.error("[QL][{}]    ↳ ChatMemory 写入失败（Redis 可能不可用）err={}", traceId, e.toString());
        }

        // ── 2. 更新 SessionState ──
        SessionState session = ctx.getSessionState();
        session.recordIntent(ctx.getIntent(), ctx.getConfidence());
        session.incrementMessages();

        if (ctx.getFinalRoute() == IntentType.GENERAL_CHAT
                && !session.isFollowUp(IntentType.GENERAL_CHAT)) {
            session.activateContext(IntentType.GENERAL_CHAT);
        }

        // ── Phase 1: 回写反馈（本轮默认采纳，后续可由 FeedbackController 覆盖） ──
        IntentFeedbackRecord pending = session.getPendingFeedback();
        if (pending != null) {
            session.recordAdoption(pending);
            session.setPendingFeedback(null);
        }

        // ── 3. 持久化 SessionState（Redis 优先 → 本地兜底） ──
        String sessionKey = SESSION_KEY_PREFIX + conversationId;
        try {
            String json = objectMapper.writeValueAsString(session);
            redisTemplate.opsForValue().set(sessionKey, json, SESSION_TTL);
            log.info("[QL][{}]    ↳ Redis 写入成功", traceId);
        } catch (Exception e) {
            log.error("[QL][{}]    ↳ Redis 写入失败，降级到本地缓存 err={}", traceId, e.toString());
            // Phase 3: Redis 不可用时写入本地内存
            localFallback.put(conversationId, session);
        }

        // Phase 3: 同时更新本地缓存（热备）
        localFallback.put(conversationId, session);

        // ★ StateBus: 广播状态变更，所有订阅者收到有序事件
        SessionState previous = stateBus.getSnapshot(conversationId);
        stateBus.publish(conversationId, previous, session, "StateWriter");

        // ── 4. 确认日志 ──
        int totalMsgs = 0;
        try {
            totalMsgs = chatMemory.get(conversationId, Integer.MAX_VALUE).size();
        } catch (Exception ignored) {}

        String feedbackSummary = "";
        if (!session.getFeedbackByIntent().isEmpty()) {
            feedbackSummary = " feedback=" + session.getFeedbackByIntent().entrySet().stream()
                    .map(e -> e.getKey() + ":" + (int)(e.getValue().getAdoptionRate() * 100) + "%")
                    .collect(java.util.stream.Collectors.joining(","));
        }

        log.info("[QL][{}]    ↳ 持久化完成 totalMessages={} lastIntent={} activeContext={} recentIntents={}{} inputPreview=\"{}\" outputPreview=\"{}\"",
                traceId, totalMsgs, session.getLastIntent(), session.getActiveContext(),
                session.getRecentIntents(), feedbackSummary,
                truncate(ctx.getCleanedInput(), 50),
                truncate(response, 80));
    }

    /**
     * Phase 3: 暴露本地缓存用于跨实例同步
     */
    public Map<String, SessionState> getLocalFallbackView() {
        return Map.copyOf(localFallback);
    }

    /**
     * Phase 1: 仅持久化 SessionState（供反馈/统计等非管线场景使用）
     */
    public void writeSessionOnly(SessionState session) {
        String conversationId = session.getUserId();
        String sessionKey = SESSION_KEY_PREFIX + conversationId;
        try {
            String json = objectMapper.writeValueAsString(session);
            redisTemplate.opsForValue().set(sessionKey, json, SESSION_TTL);
        } catch (Exception e) {
            log.error("writeSessionOnly Redis 写入失败，降级到本地缓存 err={}", e.toString());
        }
        localFallback.put(conversationId, session);

        // ★ StateBus: 广播状态变更
        SessionState previous = stateBus.getSnapshot(conversationId);
        stateBus.publish(conversationId, previous, session, "StateWriter.writeSessionOnly");
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
