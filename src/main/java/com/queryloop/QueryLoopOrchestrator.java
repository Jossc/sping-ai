package com.queryloop;

import com.queryloop.plan.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.UUID;

/**
 * 三层编排器：Router → Planner → Worker。
 *
 * <h3>架构分层</h3>
 * <pre>
 *   User Input
 *     │
 *     ▼
 *   ┌─────────────────────────────────────┐
 *   │  PreProcessor（画像加载）              │
 *   │  加载 UserProfile (Redis / Mock)     │
 *   └─────────────┬───────────────────────┘
 *                 ▼
 *   ┌─────────────────────────────────────┐
 *   │  Layer 1: Router（意图层）            │
 *   │  IntentRouterService.classify()     │
 *   │  理解用户想干什么：                    │
 *   │   ├─ 查数据？→ Query 类型            │
 *   │   ├─ 做任务？→ Task 类型             │
 *   │   └─ 写文章？→ Writing 类型          │
 *   │  输出：RouterDecision(intent, conf)  │
 *   └─────────────┬───────────────────────┘
 *                 ▼
 *   ┌─────────────────────────────────────┐
 *   │  Layer 2: Planner（规划层）           │
 *   │  PlanGenerator.generate()           │
 *   │  决定分几步走，先做什么后做什么：       │
 *   │   ├─ Step1: 查 RAG 获取背景          │
 *   │   ├─ Step2: 调用 Tool 查数据         │
 *   │   └─ Step3: 聚合结果生成回复          │
 *   │  输出：ExecutionPlan = [S1, S2, S3]  │
 *   └─────────────┬───────────────────────┘
 *                 ▼
 *   ┌─────────────────────────────────────┐
 *   │  Layer 3: Worker（执行层）            │
 *   │  MasterAgent.execute()             │
 *   │  按 Plan 顺序执行：                  │
 *   │   ├─ RAG Worker → Step1            │
 *   │   ├─ Tool Worker → Step2           │
 *   │   └─ Writer Worker → Step3          │
 *   │  输出：Flux<PlanEvent>              │
 *   └─────────────────────────────────────┘
 * </pre>
 *
 * <p>与 {@link QueryLoopService} 的关系：
 * QueryLoopService 保留原有的单意图 5-stage Pipeline，
 * QueryLoopOrchestrator 提供新的 Router→Plan→Worker 多步编排能力。
 * 两者并存，通过 Controller 的不同端点区分。</p>
 */
@Slf4j
@Service
public class QueryLoopOrchestrator {

    private final PreProcessor preProcessor;
    private final InputSanitizer inputSanitizer;
    private final IntentRouterService router;
    private final PlanGenerator planGenerator;
    private final MasterAgent worker;
    private final PlanCheckpointManager checkpointManager;

    public QueryLoopOrchestrator(PreProcessor preProcessor,
                                  InputSanitizer inputSanitizer,
                                  IntentRouterService router,
                                  PlanGenerator planGenerator,
                                  MasterAgent worker,
                                  PlanCheckpointManager checkpointManager) {
        this.preProcessor = preProcessor;
        this.inputSanitizer = inputSanitizer;
        this.router = router;
        this.planGenerator = planGenerator;
        this.worker = worker;
        this.checkpointManager = checkpointManager;
    }

    /**
     * 流式 Router → Plan → Worker 编排。
     *
     * <p>SSE 事件流：
     *   event: plan        → {"type":"plan","totalSteps":3,...}
     *   event: step_start  → {"type":"step_start","stepIndex":0,...}
     *   event: step_complete → {"type":"step_complete",...}
     *   event: complete    → {"type":"complete","summary":"..."}
     * </p>
     *
     * @param msg 用户输入
     * @param uid 用户 ID
     * @return SSE 事件流
     */
    public Flux<PlanEvent> orchestrateStream(String msg, String uid) {
        String traceId = newTraceId();
        log.info("[QLO][{}] ═══════════════════════════════════════", traceId);
        log.info("[QLO][{}] ══ Router→Plan→Worker START uid={} ══", traceId, uid);

        // ═══════════════════════════════════════════════
        // Step 0: 加载用户画像（Router 和 Planner 之前）
        // ═══════════════════════════════════════════════
        long t0 = System.currentTimeMillis();
        UserProfile profile = preProcessor.load(uid, traceId);
        log.info("[QLO][{}] Step 0: PreProcessor 完成 tier={} {}ms",
                traceId, profile.getTier(), System.currentTimeMillis() - t0);

        // ★ InputSanitizer：输入安全过滤
        long ts = System.currentTimeMillis();
        InputSanitizer.SanitizeResult sanitizeResult = inputSanitizer.sanitize(msg, uid, traceId);
        if (sanitizeResult.isBlocked()) {
            log.warn("[QLO][{}] InputSanitizer 阻断输入 reason={}", traceId, sanitizeResult.getReason());
            ExecutionPlan blockedPlan = buildBlockedPlan(msg, sanitizeResult.getReason());
            checkpointManager.savePlanSnapshot(blockedPlan, traceId);
            return worker.execute(blockedPlan, uid, traceId);
        }
        String safeInput = sanitizeResult.getSanitizedInput();
        log.info("[QLO][{}] InputSanitizer 通过 {}ms", traceId, System.currentTimeMillis() - ts);

        // ═══════════════════════════════════════════════
        // Layer 1: Router — 意图分类
        // ═══════════════════════════════════════════════
        long t1 = System.currentTimeMillis();
        RouterDecision decision = router.classify(safeInput, profile, traceId);
        log.info("[QLO][{}] Layer 1: Router 完成 intent={} confidence={} downgraded={} {}ms",
                traceId, decision.getIntent(),
                String.format("%.2f", decision.getConfidence()),
                decision.isDowngraded(),
                System.currentTimeMillis() - t1);

        // 简单查询 / 降级 → 单步执行，跳过 Planner
        if (decision.isSimpleQuery() || decision.isDowngraded()) {
            log.info("[QLO][{}] 简单查询或已降级 → 跳过 Planner，直接单步 Worker", traceId);
            ExecutionPlan singleStepPlan = buildSingleStepPlan(msg, decision);
            checkpointManager.savePlanSnapshot(singleStepPlan, traceId);
            return worker.execute(singleStepPlan, uid, traceId)
                    .doFinally(signal -> logCompletion(traceId, signal, singleStepPlan));
        }

        // ═══════════════════════════════════════════════
        // Layer 2: Planner — 生成执行计划
        // ═══════════════════════════════════════════════
        long t2 = System.currentTimeMillis();
        String planContext = decision.toPromptContext();
        ExecutionPlan plan = planGenerator.generate(msg, planContext);
        log.info("[QLO][{}] Layer 2: Planner 完成 steps={} confidence={} {}ms",
                traceId, plan.getSteps().size(),
                String.format("%.2f", plan.getPlanConfidence()),
                System.currentTimeMillis() - t2);

        // 置信度降级
        plan = applyConfidenceDegradation(plan, msg, traceId);

        // Checkpoint
        checkpointManager.savePlanSnapshot(plan, traceId);

        // ═══════════════════════════════════════════════
        // Layer 3: Worker — 执行步骤
        // ═══════════════════════════════════════════════
        log.info("[QLO][{}] Layer 3: Worker 启动 steps={}", traceId, plan.getSteps().size());
        ExecutionPlan finalPlan = plan;
        return worker.execute(plan, uid, traceId)
                .doFinally(signal -> logCompletion(traceId, signal, finalPlan));
    }

    /**
     * 从 Checkpoint 恢复流式执行。
     */
    public Flux<PlanEvent> resumeStream(String planId, String uid) {
        String traceId = newTraceId();
        log.info("[QLO][{}] ══ Router→Plan→Worker RESUME planId={} uid={} ══",
                traceId, planId, uid);

        // 恢复时仍需加载用户画像
        UserProfile profile = preProcessor.load(uid, traceId);
        log.info("[QLO][{}] PreProcessor 恢复完成 tier={}", traceId, profile.getTier());

        return worker.resume(planId, uid)
                .doFinally(signal -> log.info("[QLO][{}] ══ RESUME DONE signal={} ══",
                        traceId, signal));
    }

    /**
     * 同步 Router → Plan → Worker 编排。
     */
    public Map<String, Object> orchestrateSync(String msg, String uid) {
        String traceId = newTraceId();
        log.info("[QLO][{}] ══ Router→Plan→Worker START (sync) uid={} ══", traceId, uid);

        long t0 = System.currentTimeMillis();

        // Step 0: 用户画像
        UserProfile profile = preProcessor.load(uid, traceId);

        // ★ InputSanitizer
        InputSanitizer.SanitizeResult sanitizeResult = inputSanitizer.sanitize(msg, uid, traceId);
        if (sanitizeResult.isBlocked()) {
            return Map.of("success", false, "blocked", true,
                    "reason", sanitizeResult.getReason());
        }
        String safeInput = sanitizeResult.getSanitizedInput();

        // Layer 1: Router
        RouterDecision decision = router.classify(safeInput, profile, traceId);

        // Layer 2: Planner（简单查询跳过）
        ExecutionPlan plan;
        if (decision.isSimpleQuery() || decision.isDowngraded()) {
            plan = buildSingleStepPlan(msg, decision);
        } else {
            plan = planGenerator.generate(msg, decision.toPromptContext());
            plan = applyConfidenceDegradation(plan, msg, traceId);
        }
        checkpointManager.savePlanSnapshot(plan, traceId);

        // Layer 3: Worker（同步收集事件）
        String summary = worker.execute(plan, uid, traceId)
                .collectList()
                .map(events -> events.stream()
                        .filter(e -> "complete".equals(e.getType()))
                        .map(e -> (String) e.getPayload().get("summary"))
                        .findFirst().orElse(""))
                .block();

        long elapsed = System.currentTimeMillis() - t0;
        log.info("[QLO][{}] ══ Router→Plan→Worker DONE totalMs={} ══", traceId, elapsed);

        return Map.of(
                "success", summary != null && !summary.isEmpty(),
                "planId", plan.getPlanId(),
                "routerIntent", decision.getIntent().name(),
                "routerConfidence", decision.getConfidence(),
                "planSteps", plan.getSteps().size(),
                "planConfidence", plan.getPlanConfidence(),
                "totalMs", elapsed,
                "summary", summary != null ? summary : ""
        );
    }

    // ═══════════════════════════════════════════════
    // 辅助
    // ═══════════════════════════════════════════════

    private void logCompletion(String traceId, reactor.core.publisher.SignalType signal,
                                ExecutionPlan plan) {
        if (signal == reactor.core.publisher.SignalType.ON_COMPLETE) {
            checkpointManager.markPlanCompleted(plan.getPlanId());
        }
        log.info("[QLO][{}] ══ Router→Plan→Worker DONE signal={} ══", traceId, signal);
    }

    private ExecutionPlan applyConfidenceDegradation(ExecutionPlan plan, String msg,
                                                      String traceId) {
        if (plan.getSteps().isEmpty()) {
            log.warn("[QLO][{}] Plan 步骤数为 0，降级为单步", traceId);
            return buildSingleStepPlan(msg, null);
        }
        if (plan.getPlanConfidence() < 0.60 && plan.getSteps().size() > 1) {
            log.warn("[QLO][{}] Plan confidence 过低 ({} < 0.60)，降级为单步",
                    traceId, String.format("%.2f", plan.getPlanConfidence()));
            return buildSingleStepPlan(msg, null);
        }
        return plan;
    }

    private ExecutionPlan buildBlockedPlan(String msg, String reason) {
        ExecutionPlan plan = new ExecutionPlan()
                .setPlanId(UUID.randomUUID().toString().substring(0, 8))
                .setOriginalQuery(msg)
                .setCleanedQuery("")
                .setPlanRationale("输入被安全过滤器拦截: " + reason)
                .setPlanConfidence(1.0);

        PlanStep step = new PlanStep()
                .setStepId(UUID.randomUUID().toString().substring(0, 8))
                .setSeq(0)
                .setIntent(IntentType.GENERAL_CHAT)
                .setSubQuery("⚠️ 您的输入包含不安全内容，已被系统拦截。如有疑问请联系管理员。")
                .setReasoning("InputSanitizer 阻断，直接返回拒绝响应");

        plan.getSteps().add(step);
        plan.setStatus(PlanStatus.PLANNING);
        return plan;
    }

    private ExecutionPlan buildSingleStepPlan(String msg, RouterDecision decision) {
        ExecutionPlan plan = new ExecutionPlan()
                .setPlanId(UUID.randomUUID().toString().substring(0, 8))
                .setOriginalQuery(msg)
                .setCleanedQuery(msg.strip())
                .setPlanRationale(decision != null
                        ? "Router 分类: " + decision.getIntent()
                        : "置信度不足，降级为单步")
                .setPlanConfidence(decision != null ? decision.getConfidence() : 0.50);

        PlanStep step = new PlanStep()
                .setStepId(UUID.randomUUID().toString().substring(0, 8))
                .setSeq(0)
                .setIntent(decision != null ? decision.getIntent() : IntentType.GENERAL_CHAT)
                .setSubQuery(msg)
                .setReasoning("Router→Worker 直通模式");

        plan.getSteps().add(step);
        plan.setStatus(PlanStatus.PLANNING);
        return plan;
    }

    private String newTraceId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
