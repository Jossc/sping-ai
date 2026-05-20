package com.queryloop.plan;

import com.queryloop.IntentType;
import com.queryloop.UserTier;

/**
 * Router（意图层）的分类决策结果。
 *
 * <p>携带用户画像信息，供下游 Planner 和 Worker 做权限裁决和步骤规划。</p>
 */
public class RouterDecision {

    /** 用户原始输入 */
    private final String originalInput;

    /** 清洗后的输入 */
    private final String cleanedInput;

    /** 分类意图类型 */
    private final IntentType intent;

    /** 分类置信度 0.0~1.0 */
    private final double confidence;

    /** 用户权益等级 */
    private final UserTier tier;

    /** 是否因权益不足被降级 */
    private final boolean downgraded;

    /** 降级原因（如有） */
    private final String downgradeReason;

    public RouterDecision(String originalInput, String cleanedInput, IntentType intent,
                          double confidence, UserTier tier,
                          boolean downgraded, String downgradeReason) {
        this.originalInput = originalInput;
        this.cleanedInput = cleanedInput;
        this.intent = intent;
        this.confidence = confidence;
        this.tier = tier;
        this.downgraded = downgraded;
        this.downgradeReason = downgradeReason;
    }

    // ── 便捷方法 ──

    /** 是否为需要多步规划的复合任务 */
    public boolean isComplexTask() {
        return intent == IntentType.TOOL_CALL;
    }

    /** 是否为简单查询，可单步直接回答 */
    public boolean isSimpleQuery() {
        return intent == IntentType.GENERAL_CHAT || intent == IntentType.MEMORY_QUERY;
    }

    /** 是否需要知识库检索 */
    public boolean needsKnowledge() {
        return intent == IntentType.RAG_SEARCH;
    }

    /** 生成注入 PlanGenerator prompt 的上下文片段 */
    public String toPromptContext() {
        StringBuilder sb = new StringBuilder();
        sb.append("用户权益等级：").append(tier.name());
        sb.append("，可用功能：").append(tier.getAllowedIntents());
        if (downgraded) {
            sb.append("，[权益限制] ").append(downgradeReason);
        }
        return sb.toString();
    }

    // ── Getters ──

    public String getOriginalInput() { return originalInput; }
    public String getCleanedInput() { return cleanedInput; }
    public IntentType getIntent() { return intent; }
    public double getConfidence() { return confidence; }
    public UserTier getTier() { return tier; }
    public boolean isDowngraded() { return downgraded; }
    public String getDowngradeReason() { return downgradeReason; }
}
