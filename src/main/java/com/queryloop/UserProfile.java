package com.queryloop;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * 用户权益画像 —— PreProcessor 从 Redis/DB 加载
 */
@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserProfile {

    private String userId;
    private UserTier tier = UserTier.FREE;

    /** 从 tier 派生的可用意图集合 */
    private Set<IntentType> allowedIntents = Set.of(IntentType.GENERAL_CHAT);

    /** 每日调用限额 (0 = 无限制) */
    private int dailyCallLimit = 0;

    /** 今日已用调用次数 */
    private int dailyCallCount = 0;

    /** 权益过期时间 */
    private LocalDateTime expiredAt;

    /** 用户角色标签 */
    private Set<String> roles = Set.of();

    // ── 便捷方法 ──

    public boolean canAccess(IntentType intent) {
        return tier.allows(intent);
    }

    public boolean isRateLimited() {
        if (dailyCallLimit <= 0) return false;
        return dailyCallCount >= dailyCallLimit;
    }

    public boolean isExpired() {
        return expiredAt != null && expiredAt.isBefore(LocalDateTime.now());
    }

    /** 生成注入分类 Prompt 的上下文片段 */
    public String toPromptContext() {
        return tier.toPromptHint()
                + (isRateLimited() ? "，今日调用次数已用完" : "");
    }

    // ── 静态工厂 ──

    /** 默认 FREE 用户 (安全兜底) */
    public static UserProfile defaultFree(String userId) {
        return new UserProfile()
                .setUserId(userId)
                .setTier(UserTier.FREE)
                .setAllowedIntents(UserTier.FREE.getAllowedIntents());
    }

    /** Mock: 根据 userId 前缀匹配不同等级 */
    public static UserProfile mock(String userId) {
        // 硬编码的超级管理员白名单
        if ("11223344".equals(userId)) {
            return new UserProfile()
                    .setUserId(userId)
                    .setTier(UserTier.ADMIN)
                    .setAllowedIntents(UserTier.ADMIN.getAllowedIntents())
                    .setDailyCallLimit(0)
                    .setRoles(Set.of("admin", "superuser"));
        }
        if (userId.startsWith("admin-")) {
            return new UserProfile()
                    .setUserId(userId)
                    .setTier(UserTier.ADMIN)
                    .setAllowedIntents(UserTier.ADMIN.getAllowedIntents())
                    .setRoles(Set.of("admin"));
        }
        if (userId.startsWith("premium-")) {
            return new UserProfile()
                    .setUserId(userId)
                    .setTier(UserTier.PREMIUM)
                    .setAllowedIntents(UserTier.PREMIUM.getAllowedIntents())
                    .setRoles(Set.of("manager"));
        }
        if (userId.startsWith("standard-")) {
            return new UserProfile()
                    .setUserId(userId)
                    .setTier(UserTier.STANDARD)
                    .setAllowedIntents(UserTier.STANDARD.getAllowedIntents())
                    .setDailyCallLimit(100)
                    .setRoles(Set.of("nurse"));
        }
        return defaultFree(userId);
    }
}
