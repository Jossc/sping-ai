package com.queryloop;

import com.queryloop.plan.RouterDecision;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 独立意图路由服务 — Layer 1: Router（意图层）
 *
 * <p>三层架构中的第一层：理解用户想干什么。
 * <ul>
 *   <li>查数据？→ RAG_SEARCH / TOOL_CALL（Query 类型）</li>
 *   <li>做任务？→ TOOL_CALL（Task 类型）</li>
 *   <li>写文章/闲聊？→ GENERAL_CHAT（Writing/Chat 类型）</li>
 * </ul>
 *
 * <p>核心职责：
 * <ol>
 *   <li>接收用户输入 + 已加载的 UserProfile</li>
 *   <li>LLM 分类 → 会话裁决 → 权益裁决</li>
 *   <li>输出 {@link RouterDecision}，供下游 Planner 使用</li>
 * </ol>
 */
@Slf4j
@Service
public class IntentRouterService {

    private final InputGovernance inputGovernance;
    private final Planner planner;

    public IntentRouterService(InputGovernance inputGovernance, Planner planner) {
        this.inputGovernance = inputGovernance;
        this.planner = planner;
    }

    /**
     * Layer 1: 意图分类（Router → Planner → Worker 流程的入口）。
     *
     * <p>在调用此方法之前，PreProcessor 必须已经加载 UserProfile。
     * 本方法只做分类和裁决，不执行具体逻辑。</p>
     *
     * @param originalInput 用户原始输入
     * @param profile       已加载的用户权益画像
     * @param traceId       链路追踪 ID
     * @return 包含意图类型、置信度、权益降级信息的分类结果
     */
    public RouterDecision classify(String originalInput, UserProfile profile, String traceId) {
        // 清洗输入
        String cleaned = clean(originalInput);

        log.info("[Router][{}] ▶ Layer 1: Router 分类 userId={} tier={} input=\"{}\"",
                traceId, profile.getUserId(), profile.getTier(), truncate(cleaned, 60));

        if (cleaned.isEmpty()) {
            return new RouterDecision(originalInput, cleaned, IntentType.GENERAL_CHAT,
                    1.0, profile.getTier(), false, "");
        }

        // 构建临时上下文（只用于分类，不需要 SessionState）
        QueryLoopContext ctx = new QueryLoopContext()
                .setUserId(profile.getUserId())
                .setOriginalInput(originalInput)
                .setCleanedInput(cleaned)
                .setUserProfile(profile);
        // SessionState 初始为空（Router 层只做意图分类，不依赖历史）
        ctx.setSessionState(new SessionState());

        // 执行意图治理：LLM 分类 → 会话裁决 → 权益裁决
        inputGovernance.govern(ctx, traceId);

        RouterDecision decision = new RouterDecision(
                originalInput,
                cleaned,
                ctx.getIntent(),            // LLM 原始分类
                ctx.getConfidence(),         // 自适应置信度
                profile.getTier(),
                ctx.isDowngraded(),          // 是否被权益降级
                ctx.getRejectReason()        // 降级原因
        );

        log.info("[Router][{}] ◀ Layer 1: Router 完成 intent={} confidence={} downgraded={}",
                traceId, decision.getIntent(),
                String.format("%.2f", decision.getConfidence()),
                decision.isDowngraded());

        return decision;
    }

    /**
     * 路由并执行（兼容旧接口）
     *
     * @param ctx     已装配 UserProfile + SessionState 的上下文
     * @param traceId 链路追踪 ID
     * @return 流式响应
     */
    public reactor.core.publisher.Flux<String> routeAndExecute(QueryLoopContext ctx, String traceId) {
        inputGovernance.govern(ctx, traceId);
        return planner.planAndExecute(ctx, traceId);
    }

    /**
     * 获取当前实例的路由决策摘要（用于跨实例调试）
     */
    public String getRouteDecision(QueryLoopContext ctx) {
        return String.format("userId=%s classified=%s finalRoute=%s confidence=%.2f",
                ctx.getUserId(), ctx.getIntent(), ctx.getFinalRoute(), ctx.getConfidence());
    }

    private String clean(String input) {
        if (input == null) return "";
        return input.strip().replaceAll("\\s{2,}", " ");
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
