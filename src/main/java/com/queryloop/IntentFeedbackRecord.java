package com.queryloop;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/**
 * 单条意图反馈记录
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class IntentFeedbackRecord {

    /** 反馈时间 */
    private LocalDateTime timestamp = LocalDateTime.now();

    /** 发生该反馈的会话 ID */
    private String conversationId;

    /** LLM 原始分类结果 */
    private IntentType classifiedIntent;

    /** 最终执行的路由（经裁决后） */
    private IntentType finalRoute;

    /** 分类置信度 */
    private double confidence;

    /** 用户采纳结果 */
    private IntentAdoption adoption = IntentAdoption.ADOPTED;

    /** 如果 adoption=CORRECTED，用户指定的正确意图 */
    private IntentType correctedIntent;

    /** 用户输入摘要（截取前 60 字，避免 Redis Key 膨胀） */
    private String inputSnippet;

    // ── 工厂方法 ──

    public static IntentFeedbackRecord fromContext(QueryLoopContext ctx, IntentAdoption adoption) {
        return new IntentFeedbackRecord()
                .setTimestamp(LocalDateTime.now())
                .setConversationId(ctx.getUserId())
                .setClassifiedIntent(ctx.getIntent())
                .setFinalRoute(ctx.getFinalRoute())
                .setConfidence(ctx.getConfidence())
                .setAdoption(adoption)
                .setInputSnippet(ctx.getCleanedInput() != null
                        ? ctx.getCleanedInput().substring(0, Math.min(ctx.getCleanedInput().length(), 60))
                        : "");
    }

    public static IntentFeedbackRecord corrected(QueryLoopContext ctx, IntentType correctIntent) {
        IntentFeedbackRecord record = fromContext(ctx, IntentAdoption.CORRECTED);
        record.setCorrectedIntent(correctIntent);
        return record;
    }
}
