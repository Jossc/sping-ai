package com.queryloop.plan;

import com.queryloop.PreProcessor;
import com.queryloop.UserProfile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import java.util.*;

@Slf4j
@Service
public class PlanExecuteService {

    private final PreProcessor preProcessor;
    private final PlanGenerator planGenerator;
    private final ExecuteOrchestrator orchestrator;
    private final PlanCheckpointManager checkpointManager;

    /** Plan 置信度低于此阈值且步骤数 > 1 时，降级为单步执行 */
    private static final double MIN_PLAN_CONFIDENCE = 0.60;

    public PlanExecuteService(PreProcessor preProcessor,
                              PlanGenerator planGenerator,
                              ExecuteOrchestrator orchestrator,
                              PlanCheckpointManager checkpointManager) {
        this.preProcessor = preProcessor;
        this.planGenerator = planGenerator;
        this.orchestrator = orchestrator;
        this.checkpointManager = checkpointManager;
    }

    /**
     * 流式 Plan-and-Execute。
     * SSE 事件流：plan → step_start → step_complete → ... → complete
     */
    public Flux<PlanEvent> executeStream(String msg, String uid) {
        String traceId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[PE][{}] ══════ Plan-Execute START (stream) uid={} ══════", traceId, uid);

        // 1. 加载用户信息
        UserProfile profile = preProcessor.load(uid, traceId);

        // 2. Plan Phase：LLM 拆解
        ExecutionPlan plan = planGenerator.generate(msg, profile.toPromptContext());
        log.info("[PE][{}] Plan: {} steps rationale={} confidence={}",
                traceId, plan.getSteps().size(), plan.getPlanRationale(), plan.getPlanConfidence());

        // 3. 置信度判断 + 降级策略
        plan = applyConfidenceDegradation(plan, msg, profile, traceId);

        // 4. ★ Checkpoint：Plan 生成后立即持久化
        checkpointManager.savePlanSnapshot(plan, traceId);

        // 5. Execute Phase
        final ExecutionPlan finalPlan = plan;
        return orchestrator.executeStream(plan, uid, traceId)
                .doFinally(signal -> {
                    if (signal == SignalType.ON_COMPLETE) {
                        checkpointManager.markPlanCompleted(finalPlan.getPlanId());
                    }
                    log.info("[PE][{}] ══════ Plan-Execute DONE signal={} ══════",
                            traceId, signal);
                });
    }

    /**
     * 从 checkpoint 恢复流式执行。
     * 用于客户端重连场景：传入上次的 planId，从断点继续。
     */
    public Flux<PlanEvent> resumeStream(String planId, String uid) {
        String traceId = UUID.randomUUID().toString().substring(0, 8);

        PlanCheckpointManager.PlanRecoveryResult recovery = checkpointManager.tryRecover(planId);

        if (!recovery.isRecoverable()) {
            log.warn("[PE][{}] Plan {} 无法恢复（不存在或已完成）", traceId, planId);
            return Flux.error(new IllegalStateException(
                    "Plan " + planId + " 无法恢复：不存在或已执行完成"));
        }

        log.info("[PE][{}] ══════ Plan-Execute RESUME planId={} fromStep={} uid={} ══════",
                traceId, planId, recovery.getResumeFromSeq(), uid);

        // 恢复时重新加载用户信息（权益可能在间隔内发生变化）
        UserProfile profile = preProcessor.load(uid, traceId);

        return orchestrator.executeStreamWithRecovery(
                        recovery.getPlan(), uid, traceId, recovery)
                .doFinally(signal -> {
                    if (signal == SignalType.ON_COMPLETE) {
                        checkpointManager.markPlanCompleted(planId);
                    }
                    log.info("[PE][{}] ══════ Plan-Execute RESUME DONE signal={} ══════",
                            traceId, signal);
                });
    }

    /**
     * 同步 Plan-and-Execute。
     * 阻塞等待全部步骤执行完毕，返回完整结果 JSON。
     */
    public Map<String, Object> executeSync(String msg, String uid) {
        String traceId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[PE][{}] ══════ Plan-Execute START (sync) uid={} ══════", traceId, uid);

        long t0 = System.currentTimeMillis();
        UserProfile profile = preProcessor.load(uid, traceId);
        ExecutionPlan plan = planGenerator.generate(msg, profile.toPromptContext());
        log.info("[PE][{}] Plan: {} steps rationale={} confidence={}",
                traceId, plan.getSteps().size(), plan.getPlanRationale(), plan.getPlanConfidence());

        // 置信度降级
        plan = applyConfidenceDegradation(plan, msg, profile, traceId);

        // Checkpoint
        checkpointManager.savePlanSnapshot(plan, traceId);

        // 3. Execute Phase (blocking)
        String summary = orchestrator.executeSync(plan, uid, traceId);
        long elapsed = System.currentTimeMillis() - t0;

        checkpointManager.markPlanCompleted(plan.getPlanId());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", summary != null && !summary.isEmpty());
        result.put("planId", plan.getPlanId());
        result.put("planRationale", plan.getPlanRationale());
        result.put("planConfidence", plan.getPlanConfidence());
        result.put("totalSteps", plan.getSteps().size());
        result.put("completedSteps", plan.getSteps().stream()
                .filter(s -> s.getStatus() == StepStatus.COMPLETED).count());
        result.put("failedSteps", plan.getSteps().stream()
                .filter(s -> s.getStatus() == StepStatus.FAILED).count());
        result.put("replanCount", plan.getReplanCount());
        result.put("totalMs", elapsed);

        // 各步骤明细
        List<Map<String, Object>> stepDetails = new ArrayList<>();
        for (PlanStep s : plan.getSteps()) {
            Map<String, Object> sd = new LinkedHashMap<>();
            sd.put("seq", s.getSeq());
            sd.put("intent", s.getIntent().name());
            sd.put("subQuery", s.getSubQuery());
            sd.put("status", s.getStatus().name());
            sd.put("result", s.getResult());
            if (s.getCompletedAt() > 0) {
                sd.put("elapsedMs", s.getCompletedAt() - s.getStartedAt());
            }
            stepDetails.add(sd);
        }
        result.put("steps", stepDetails);
        result.put("summary", summary);
        return result;
    }

    // ═══════════════════════════════════════════════
    // 置信度降级策略
    // ═══════════════════════════════════════════════

    /**
     * 当 LLM 规划的置信度不足时，降级为安全的单步执行。
     *
     * 降级规则：
     *   1. confidence < 0.60 且 steps > 1 → 强制降级为单步 GENERAL_CHAT
     *   2. steps == 0（LLM 返回空）→ 兜底单步
     *   3. 否则保持原 plan
     *
     * 注意：不再重新调用 planGenerator.generate()，因为二次调用仍可能
     * 返回低置信度结果，存在无限降级风险。
     */
    private ExecutionPlan applyConfidenceDegradation(ExecutionPlan plan, String msg,
                                                      UserProfile profile, String traceId) {
        // 空步骤兜底
        if (plan.getSteps().isEmpty()) {
            log.warn("[PE][{}] Plan 步骤数为 0，降级为单步执行", traceId);
            return buildSingleStepPlan(msg);
        }

        // 低置信度 + 多步骤 → 强制单步
        if (plan.getPlanConfidence() < MIN_PLAN_CONFIDENCE && plan.getSteps().size() > 1) {
            log.warn("[PE][{}] Plan confidence 过低 ({} < {})，{} steps → 降级为单步执行",
                    traceId, plan.getPlanConfidence(), MIN_PLAN_CONFIDENCE, plan.getSteps().size());
            return buildSingleStepPlan(msg);
        }

        return plan;
    }

    /**
     * 构造安全的单步执行计划（GENERAL_CHAT）。
     * 不依赖 LLM，确保降级路径可靠。
     */
    private ExecutionPlan buildSingleStepPlan(String msg) {
        ExecutionPlan plan = new ExecutionPlan()
                .setPlanId(UUID.randomUUID().toString().substring(0, 8))
                .setOriginalQuery(msg)
                .setCleanedQuery(msg.strip())
                .setPlanRationale("置信度不足，降级为单步对话")
                .setPlanConfidence(0.50);

        PlanStep step = new PlanStep()
                .setStepId(UUID.randomUUID().toString().substring(0, 8))
                .setSeq(0)
                .setIntent(com.queryloop.IntentType.GENERAL_CHAT)
                .setSubQuery(msg)
                .setReasoning("兜底降级：保持原始查询以单步执行");

        plan.getSteps().add(step);
        plan.setStatus(PlanStatus.PLANNING);
        return plan;
    }
}
