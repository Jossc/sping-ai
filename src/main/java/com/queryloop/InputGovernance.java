package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class InputGovernance {

    private final ChatModel chatModel;

    private static final String CLASSIFICATION_PROMPT = """
            你是一个意图分类器。分析用户输入，只输出意图标签，不要解释。

            意图标签定义：
            - TOOL_CALL：需要调用外部工具（如查询股票、获取时间、触发报警等）
            - RAG_SEARCH：需要检索知识库中的专业文档
            - MEMORY_QUERY：询问历史对话中提到过的内容
            - GENERAL_CHAT：普通闲聊或不需特殊处理的对话

            %s
            %s
            上一轮意图：%s
            当前活跃上下文：%s
            用户输入：%s
            意图：""";

    public InputGovernance(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    public void govern(QueryLoopContext ctx, String traceId) {
        String cleaned = clean(ctx.getOriginalInput());
        ctx.setCleanedInput(cleaned);

        log.info("[QL][{}] ▶ 2/5 InputGovernance rawLen={} cleanedLen={}",
                traceId,
                ctx.getOriginalInput() != null ? ctx.getOriginalInput().length() : 0,
                cleaned.length());

        if (cleaned.isEmpty()) {
            ctx.setIntent(IntentType.GENERAL_CHAT)
               .setFinalRoute(IntentType.GENERAL_CHAT)
               .setConfidence(1.0)
               .setRejectReason("");
            return;
        }

        SessionState session = ctx.getSessionState();
        UserProfile profile = ctx.getUserProfile();

        // Step A: LLM 分类（注入权益上下文 + 反馈统计提示）
        IntentType classified = classifyIntent(cleaned, session, profile, traceId);

        // Step B: 会话上下文裁决
        IntentType sessionResolved = resolveBySession(classified, session, cleaned, traceId);

        // Step C: 权益裁决
        IntentType finalRoute = resolveByEntitlement(sessionResolved, profile, cleaned, traceId);
        String rejectReason = buildRejectReason(sessionResolved, finalRoute, profile);

        ctx.setIntent(classified);
        ctx.setFinalRoute(finalRoute);
        ctx.setRejectReason(rejectReason != null ? rejectReason : "");

        // ── Phase 1: 自适应置信度 —— 基于历史反馈调整 ──
        double baseConf = computeConfidence(classified, finalRoute, session);
        double penalty = session.getConfidencePenalty(classified);
        double adjustedConf = Math.round(baseConf * penalty * 100.0) / 100.0;
        ctx.setConfidence(adjustedConf);

        if (penalty < 1.0) {
            log.info("[QL][{}]    ↳ 自适应置信度: base={} penalty={} adjusted={} adoptionRate={}%",
                    traceId, String.format("%.2f", baseConf), String.format("%.2f", penalty),
                    String.format("%.2f", adjustedConf),
                    String.format("%.0f", session.getAdoptionRate(classified) * 100));
        }

        // ── Phase 1: 记录待反馈分类结果 ──
        IntentFeedbackRecord pending = IntentFeedbackRecord.fromContext(ctx, IntentAdoption.ADOPTED);
        session.setPendingFeedback(pending);

        log.info("[QL][{}]    ↳ classified={} sessionResolved={} finalRoute={} downgraded={} tier={} confidence={}(adj) input=\"{}\"",
                traceId, classified, sessionResolved, finalRoute,
                ctx.isDowngraded(), profile.getTier(), String.format("%.2f", ctx.getConfidence()),
                truncate(cleaned, 80));
    }

    /**
     * Plan-and-Execute 专用：使用计划中预分类的意图，跳过 LLM 分类（~800ms 节省）。
     * 会话裁决和权益裁决仍然执行（不依赖 LLM，延迟可忽略）。
     */
    public void governWithHint(QueryLoopContext ctx, IntentType preClassified, String traceId) {
        String cleaned = clean(ctx.getOriginalInput());
        ctx.setCleanedInput(cleaned);

        log.info("[QL][{}] ▶ 2/5 InputGovernance (plan-hint) preClassified={} input=\"{}\"",
                traceId, preClassified, truncate(cleaned, 50));

        if (cleaned.isEmpty()) {
            ctx.setIntent(IntentType.GENERAL_CHAT)
               .setFinalRoute(IntentType.GENERAL_CHAT)
               .setConfidence(1.0);
            return;
        }

        SessionState session = ctx.getSessionState();
        UserProfile profile = ctx.getUserProfile();

        // 跳过 LLM 分类，直接使用 plan 预分类
        IntentType classified = preClassified;

        // Step B + C 保留（不依赖 LLM）
        IntentType sessionResolved = resolveBySession(classified, session, cleaned, traceId);
        IntentType finalRoute = resolveByEntitlement(sessionResolved, profile, cleaned, traceId);
        String rejectReason = buildRejectReason(sessionResolved, finalRoute, profile);

        ctx.setIntent(classified);
        ctx.setFinalRoute(finalRoute);
        ctx.setRejectReason(rejectReason != null ? rejectReason : "");

        double baseConf = computeConfidence(classified, finalRoute, session);
        double penalty = session.getConfidencePenalty(classified);
        ctx.setConfidence(Math.round(baseConf * penalty * 100.0) / 100.0);

        log.info("[QL][{}]    ↳ plan-hint classified={} finalRoute={} confidence={}",
                traceId, classified, finalRoute, String.format("%.2f", ctx.getConfidence()));
    }

    // ═══ Step A: LLM 分类（注入反馈提示） ═══
    private IntentType classifyIntent(String input, SessionState session,
                                       UserProfile profile, String traceId) {
        try {
            long t0 = System.currentTimeMillis();
            // 获取该意图类型的历史反馈提示
            String feedbackHint = session.getFeedbackByIntent() != null
                    ? buildFeedbackHint(session) : "";

            String prompt = String.format(CLASSIFICATION_PROMPT,
                    profile.toPromptContext(),
                    feedbackHint,                                // 反馈提示
                    session.getLastIntent().name(),
                    session.getActiveContext().name(),
                    input);
            String raw = chatModel.call(new Prompt(prompt))
                    .getResult()
                    .getOutput()
                    .getContent();
            long elapsed = System.currentTimeMillis() - t0;
            log.info("[QL][{}]    ↳ 分类 LLM {}ms raw=\"{}\"", traceId, elapsed, truncate(raw, 60));
            return parseIntent(raw);
        } catch (Exception e) {
            log.warn("[QL][{}]    ↳ 分类异常，fallback=GENERAL_CHAT err={}", traceId, e.toString());
            return IntentType.GENERAL_CHAT;
        }
    }

    /** 构建反馈统计提示语 */
    private String buildFeedbackHint(SessionState session) {
        StringBuilder sb = new StringBuilder();
        Map<String, IntentFeedbackStats> stats = session.getFeedbackByIntent();
        for (Map.Entry<String, IntentFeedbackStats> entry : stats.entrySet()) {
            IntentFeedbackStats s = entry.getValue();
            if (s.needsConfidencePenalty()) {
                sb.append(s.toPromptHint()).append("\n");
            }
        }
        return sb.toString();
    }

    // ═══ Step B: 会话上下文裁决 ═══
    private IntentType resolveBySession(IntentType classified, SessionState session,
                                         String input, String traceId) {
        boolean isFollowUp = isFollowUpPhrasing(input);

        if (classified == IntentType.MEMORY_QUERY
                && session.isToolCallContinuation() && isFollowUp) {
            log.info("[QL][{}]    ↳ 会话裁决: MEMORY_QUERY → TOOL_CALL", traceId);
            return IntentType.TOOL_CALL;
        }
        if (classified == IntentType.MEMORY_QUERY
                && session.isRagContinuation() && isFollowUp) {
            log.info("[QL][{}]    ↳ 会话裁决: MEMORY_QUERY → RAG_SEARCH", traceId);
            return IntentType.RAG_SEARCH;
        }
        return classified;
    }

    // ═══ Step C: 权益裁决 ═══
    private IntentType resolveByEntitlement(IntentType resolved, UserProfile profile,
                                             String input, String traceId) {
        if (profile.canAccess(resolved)) return resolved;

        if (resolved == IntentType.TOOL_CALL) {
            log.warn("[QL][{}]    ↳ 权益裁决: HARD_REJECT TOOL_CALL tier={}", traceId, profile.getTier());
            return IntentType.TOOL_CALL;
        }
        if (resolved == IntentType.RAG_SEARCH) {
            log.info("[QL][{}]    ↳ 权益裁决: DOWNGRADE RAG_SEARCH → GENERAL_CHAT", traceId);
            return IntentType.GENERAL_CHAT;
        }
        if (resolved == IntentType.MEMORY_QUERY) {
            log.info("[QL][{}]    ↳ 权益裁决: SILENT_DOWNGRADE MEMORY_QUERY → GENERAL_CHAT", traceId);
            return IntentType.GENERAL_CHAT;
        }
        return IntentType.GENERAL_CHAT;
    }

    // ═══ 辅助方法 ═══
    private String buildRejectReason(IntentType before, IntentType after, UserProfile profile) {
        if (before == after) return "";
        if (before == IntentType.TOOL_CALL) {
            return "工具调用需要 PREMIUM 及以上权益（当前: " + profile.getTier() + "）";
        }
        if (before == IntentType.RAG_SEARCH) {
            return "知识库检索需要 STANDARD 及以上权益（当前: " + profile.getTier() + "）";
        }
        return "";
    }

    private double computeConfidence(IntentType classified, IntentType finalRoute, SessionState session) {
        if (classified == finalRoute && session.isSameIntent(classified)) return 0.95;
        if (classified == finalRoute) return 0.85;
        return 0.70;
    }

    private boolean isFollowUpPhrasing(String input) {
        String[] patterns = {
                "刚才", "刚刚", "那个", "这个", "它", "他", "她",
                "还有", "继续", "然后", "那", "之前的", "上次",
                "再", "也", "呢", "吗", "怎么", "为什么"
        };
        for (String p : patterns) {
            if (input.contains(p)) return true;
        }
        return input.length() <= 8;
    }

    private String clean(String input) {
        if (input == null) return "";
        return input.strip().replaceAll("\\s{2,}", " ");
    }

    private IntentType parseIntent(String raw) {
        if (raw == null) return IntentType.GENERAL_CHAT;
        String upper = raw.strip().toUpperCase();
        for (IntentType intent : IntentType.values()) {
            if (upper.contains(intent.name())) return intent;
        }
        return IntentType.GENERAL_CHAT;
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
