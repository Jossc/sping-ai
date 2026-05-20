package com.queryloop;

import java.util.Set;

/**
 * 用户权益等级 → 可用意图白名单
 */
public enum UserTier {

    /** 免费用户：仅基础对话 */
    FREE(Set.of(IntentType.GENERAL_CHAT)),

    /** 标准用户：对话 + 知识库 + 记忆回溯 */
    STANDARD(Set.of(IntentType.GENERAL_CHAT, IntentType.RAG_SEARCH, IntentType.MEMORY_QUERY)),

    /** 高级用户：全部功能 */
    PREMIUM(Set.of(IntentType.values())),

    /** 管理员：全部功能 + 无限制 */
    ADMIN(Set.of(IntentType.values()));

    private final Set<IntentType> allowedIntents;

    UserTier(Set<IntentType> allowedIntents) {
        this.allowedIntents = Set.copyOf(allowedIntents);
    }

    /** 该等级是否允许使用某意图 */
    public boolean allows(IntentType intent) {
        return allowedIntents.contains(intent);
    }

    /** 获取所有可用意图 */
    public Set<IntentType> getAllowedIntents() {
        return allowedIntents;
    }

    /** 获取被禁止的意图列表 */
    public Set<IntentType> getDisallowedIntents() {
        Set<IntentType> all = Set.of(IntentType.values());
        return all.stream()
                .filter(i -> !allowedIntents.contains(i))
                .collect(java.util.stream.Collectors.toSet());
    }

    /** 生成 LLM 分类 Prompt 中的权益说明片段 */
    public String toPromptHint() {
        return "用户权益等级：" + name()
                + "，可用功能：" + allowedIntents
                + (getDisallowedIntents().isEmpty() ? "" : "，不可用功能：" + getDisallowedIntents());
    }
}
