package com.queryloop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 管线第 0 步 —— 加载用户权益画像
 *
 * 加载优先级:
 *   1. Redis "user:{userId}:profile"  (热数据, ~1ms)
 *   2. Mock 兜底                       (开发环境)
 *   3. 全未命中 → defaultFree()        (安全默认: 最小权限)
 */
@Slf4j
@Component
public class PreProcessor {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String USER_PROFILE_KEY_PREFIX = "user:";
    private static final String USER_PROFILE_KEY_SUFFIX = ":profile";

    /** 是否启用 Mock 模式 (Redis 未命中时使用) */
    private boolean mockMode = true;

    public PreProcessor(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    /**
     * 加载用户权益画像
     */
    public UserProfile load(String userId, String traceId) {
        log.info("[QL][{}] ▶ 0/5 PreProcessor userId={}", traceId, userId);

        // 1. Redis 热数据
        String redisKey = USER_PROFILE_KEY_PREFIX + userId + USER_PROFILE_KEY_SUFFIX;
        try {
            String json = redisTemplate.opsForValue().get(redisKey);
            if (json != null && !json.isEmpty()) {
                UserProfile profile = objectMapper.readValue(json, UserProfile.class);
                log.info("[QL][{}]    ↳ Redis 命中 tier={} allowedIntents={} dailyCallCount={}",
                        traceId, profile.getTier(), profile.getAllowedIntents(), profile.getDailyCallCount());
                return validateAndReturn(profile, traceId);
            }
        } catch (Exception e) {
            log.warn("[QL][{}]    ↳ Redis 反序列化失败，降级到 Mock err={}", traceId, e.toString());
        }

        // 2. Mock 模式 (开发环境)
        if (mockMode) {
            UserProfile profile = UserProfile.mock(userId);
            log.info("[QL][{}]    ↳ Mock 模式 tier={} allowedIntents={} (userId 前缀匹配)",
                    traceId, profile.getTier(), profile.getAllowedIntents());
            // Mock 结果也写回 Redis (短 TTL 避免穿透)
            cacheProfile(redisKey, profile);
            return validateAndReturn(profile, traceId);
        }

        // 3. 兜底: 默认 FREE
        log.warn("[QL][{}]    ↳ 全未命中，兜底为 FREE", traceId);
        return UserProfile.defaultFree(userId);
    }

    /**
     * 校验并返回: 过期/限流检测
     */
    private UserProfile validateAndReturn(UserProfile profile, String traceId) {
        if (profile.isExpired()) {
            log.warn("[QL][{}]    ↳ 权益已过期 expiredAt={}，降级为 FREE", traceId, profile.getExpiredAt());
            return UserProfile.defaultFree(profile.getUserId());
        }
        if (profile.isRateLimited()) {
            log.warn("[QL][{}]    ↳ 今日调用次数已用完 {}/{}，但仍允许 GENERAL_CHAT",
                    traceId, profile.getDailyCallCount(), profile.getDailyCallLimit());
            // 超限只限工具/RAG，仍允许基础对话
        }
        return profile;
    }

    /**
     * Mock 结果回写 Redis (短 TTL, 避免下次 Mock)
     */
    private void cacheProfile(String key, UserProfile profile) {
        try {
            String json = objectMapper.writeValueAsString(profile);
            redisTemplate.opsForValue().set(key, json, java.time.Duration.ofMinutes(5));
        } catch (Exception ignored) {
        }
    }
}
