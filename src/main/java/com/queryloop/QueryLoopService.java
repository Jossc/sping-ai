package com.queryloop;

import com.queryloop.plan.PlanEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
public class QueryLoopService {

    private final PreProcessor preProcessor;
    private final StateReader stateReader;
    private final InputSanitizer inputSanitizer;
    private final InputGovernance inputGovernance;
    private final Planner planner;
    private final OutputAuditor outputAuditor;
    private final StateWriter stateWriter;
    private final QueryLoopOrchestrator orchestrator;

    public QueryLoopService(PreProcessor preProcessor,
                            StateReader stateReader,
                            InputSanitizer inputSanitizer,
                            InputGovernance inputGovernance,
                            Planner planner,
                            OutputAuditor outputAuditor,
                            StateWriter stateWriter,
                            QueryLoopOrchestrator orchestrator) {
        this.preProcessor = preProcessor;
        this.stateReader = stateReader;
        this.inputSanitizer = inputSanitizer;
        this.inputGovernance = inputGovernance;
        this.planner = planner;
        this.outputAuditor = outputAuditor;
        this.stateWriter = stateWriter;
        this.orchestrator = orchestrator;
    }

    // ═══════════════════════════════════════════════
    // 流式执行: PreProcessor → StateReader → InputGovernance → Planner → StateWriter
    // （原有单意图 5-stage Pipeline，保持兼容）
    // ═══════════════════════════════════════════════
    public Flux<String> execute(String rawInput, String userId) {
        String traceId = newTraceId();
        long t0 = System.currentTimeMillis();
        log.info("[QL][{}] ══════ Query Loop START (stream) userId={} rawLen={} ══════",
                traceId, userId, rawInput != null ? rawInput.length() : 0);

        // ── 管线装配 ──
        QueryLoopContext ctx = assemblePipeline(rawInput, userId, traceId, t0);

        // ── 流式执行 + 写回 ──
        long t3 = System.currentTimeMillis();
        StringBuilder collector = new StringBuilder();
        return planner.planAndExecute(ctx, traceId)
                .doOnNext(collector::append)
                .doFinally(signal -> {
                    logStage(traceId, "3/5 Planner (stream " + signal + ")", t3);

                    if (collector.length() > 0) {
                        // ★ OutputAuditor：LLM 输出安全审计
                        long ta = System.currentTimeMillis();
                        OutputAuditor.AuditResult auditResult = outputAuditor.audit(
                                collector.toString(), userId, traceId);
                        ctx.setResponse(auditResult.getAuditedOutput());
                        logStage(traceId, "3.5/5 OutputAuditor", ta);

                        long t4 = System.currentTimeMillis();
                        stateWriter.writeState(ctx, traceId);
                        logStage(traceId, "4/5 StateWriter", t4);
                    } else {
                        log.warn("[QL][{}]    ↳ 空响应，跳过 StateWriter", traceId);
                    }

                    long total = System.currentTimeMillis() - t0;
                    log.info("[QL][{}] ══════ Query Loop DONE totalMs={} responseLen={} intent={} finalRoute={} tier={} downgraded={} ══════",
                            traceId, total, collector.length(),
                            ctx.getIntent(), ctx.getFinalRoute(),
                            ctx.getUserProfile().getTier(), ctx.isDowngraded());
                });
    }

    // ═══════════════════════════════════════════════
    // 同步执行（原有单意图）
    // ═══════════════════════════════════════════════
    public String executeSync(String rawInput, String userId) {
        String traceId = newTraceId();
        long t0 = System.currentTimeMillis();
        log.info("[QL][{}] ══════ Query Loop START (sync) userId={} rawLen={} ══════",
                traceId, userId, rawInput != null ? rawInput.length() : 0);

        // ── 管线装配 ──
        QueryLoopContext ctx = assemblePipeline(rawInput, userId, traceId, t0);
        long t3 = System.currentTimeMillis();

        // ── 规划执行 ──
        String response = planner.planAndExecute(ctx, traceId)
                .collectList()
                .map(list -> String.join("", list))
                .block();
        logStage(traceId, "3/5 Planner", t3);

        // ★ OutputAuditor：输出安全审计
        long ta = System.currentTimeMillis();
        OutputAuditor.AuditResult auditResult = outputAuditor.audit(
                response != null ? response : "", userId, traceId);
        String auditedResponse = auditResult.getAuditedOutput();
        logStage(traceId, "3.5/5 OutputAuditor", ta);

        // ── 状态更新 ──
        long t4 = System.currentTimeMillis();
        ctx.setResponse(auditedResponse);
        stateWriter.writeState(ctx, traceId);
        logStage(traceId, "4/5 StateWriter", t4);

        long total = System.currentTimeMillis() - t0;
        log.info("[QL][{}] ══════ Query Loop DONE totalMs={} responseLen={} intent={} finalRoute={} tier={} downgraded={} ══════",
                traceId, total, ctx.getResponse().length(),
                ctx.getIntent(), ctx.getFinalRoute(),
                ctx.getUserProfile().getTier(), ctx.isDowngraded());
        return ctx.getResponse();
    }

    // ═══════════════════════════════════════════════
    // Router → Plan → Worker 流式编排（新增三层架构）
    // ═══════════════════════════════════════════════

    /**
     * 流式 Router → Plan → Worker 编排。
     *
     * <p>与 {@link #execute} (SSE 文本流) 不同，本方法返回结构化的 {@link PlanEvent} 事件流，
     * 前端可据此渲染步骤进度 UI。</p>
     *
     * @param msg 用户输入
     * @param uid 用户 ID
     * @return PlanEvent SSE 事件流
     */
    public Flux<PlanEvent> executeOrchestrated(String msg, String uid) {
        String traceId = newTraceId();
        log.info("[QL][{}] ══════ Query Loop ORCHESTRATED START uid={} ══════", traceId, uid);

        return orchestrator.orchestrateStream(msg, uid)
                .doOnComplete(() -> {
                    // 编排完成后写会话状态
                    writeSessionAfterOrchestration(uid, traceId);
                })
                .doFinally(signal -> {
                    log.info("[QL][{}] ══════ Query Loop ORCHESTRATED DONE signal={} ══════",
                            traceId, signal);
                });
    }

    /**
     * 同步 Router → Plan → Worker 编排。
     */
    public Map<String, Object> executeOrchestratedSync(String msg, String uid) {
        String traceId = newTraceId();
        log.info("[QL][{}] ══════ Query Loop ORCHESTRATED START (sync) uid={} ══════", traceId, uid);

        Map<String, Object> result = orchestrator.orchestrateSync(msg, uid);

        writeSessionAfterOrchestration(uid, traceId);

        log.info("[QL][{}] ══════ Query Loop ORCHESTRATED DONE (sync) ══════", traceId);
        return result;
    }

    /**
     * 从 Checkpoint 恢复编排执行。
     */
    public Flux<PlanEvent> resumeOrchestrated(String planId, String uid) {
        String traceId = newTraceId();
        log.info("[QL][{}] ══════ Query Loop ORCHESTRATED RESUME planId={} uid={} ══════",
                traceId, planId, uid);

        return orchestrator.resumeStream(planId, uid)
                .doOnComplete(() -> writeSessionAfterOrchestration(uid, traceId))
                .doFinally(signal -> {
                    log.info("[QL][{}] ══════ Query Loop ORCHESTRATED RESUME DONE signal={} ══════",
                            traceId, signal);
                });
    }

    /**
     * 编排完成后写会话状态（保持与 StateWriter 的集成）。
     */
    private void writeSessionAfterOrchestration(String uid, String traceId) {
        try {
            SessionState session = stateReader.getSessionState(uid);
            stateWriter.writeSessionOnly(session);
            log.debug("[QL][{}] 编排完成后会话状态已持久化 uid={}", traceId, uid);
        } catch (Exception e) {
            log.warn("[QL][{}] 编排完成后状态写入失败 uid={} err={}", traceId, uid, e.toString());
        }
    }

    // ═══════════════════════════════════════════════
    // Phase 1: 意图反馈
    // ═══════════════════════════════════════════════

    /**
     * 记录用户对意图分类的纠正反馈
     */
    public Map<String, Object> recordCorrection(String uid, IntentType correctIntent) {
        SessionState session = stateReader.getSessionState(uid);
        IntentFeedbackRecord corrected = new IntentFeedbackRecord()
                .setTimestamp(java.time.LocalDateTime.now())
                .setConversationId(uid)
                .setAdoption(IntentAdoption.CORRECTED)
                .setCorrectedIntent(correctIntent);
        session.recordCorrection(corrected);

        // 写回
        try {
            stateWriter.writeSessionOnly(session);
        } catch (Exception e) {
            log.error("[QL] recordCorrection 写回失败 uid={} err={}", uid, e.toString());
        }

        IntentFeedbackStats stats = session.getFeedbackByIntent().get(correctIntent.name());
        Map<String, Object> result = new HashMap<>();
        result.put("uid", uid);
        result.put("correctedIntent", correctIntent.name());
        result.put("totalCorrections", stats != null ? stats.getCorrectedCount() : 0);
        result.put("adoptionRate", stats != null ? stats.getAdoptionRate() : 1.0);
        return result;
    }

    /**
     * 查询某用户当前会话的意图反馈统计
     */
    public Map<String, Object> getFeedbackStats(String uid) {
        SessionState session = stateReader.getSessionState(uid);
        Map<String, Object> result = new HashMap<>();
        result.put("uid", uid);
        result.put("lastIntent", session.getLastIntent().name());
        result.put("totalMessages", session.getTotalMessages());

        Map<String, Map<String, Object>> byIntent = new HashMap<>();
        for (Map.Entry<String, IntentFeedbackStats> entry : session.getFeedbackByIntent().entrySet()) {
            IntentFeedbackStats s = entry.getValue();
            Map<String, Object> detail = new HashMap<>();
            detail.put("total", s.getTotalCount());
            detail.put("adopted", s.getAdoptedCount());
            detail.put("corrected", s.getCorrectedCount());
            detail.put("ignored", s.getIgnoredCount());
            detail.put("adoptionRate", s.getAdoptionRate());
            detail.put("needsPenalty", s.needsConfidencePenalty());
            byIntent.put(entry.getKey(), detail);
        }
        result.put("byIntent", byIntent);
        return result;
    }

    // ═══════════════════════════════════════════════
    // Plan-and-Execute: 使用已装配 context 执行同步 Pipeline
    // ═══════════════════════════════════════════════

    /**
     * Plan-and-Execute 专用：使用已部分装配的 context 执行同步 Pipeline。
     * PreProcessor + StateReader 已在上层完成，这里走 InputGovernance → Planner → StateWriter。
     *
     * 如果 ctx.metadata 中包含 skipLLMClassification=true + planPreClassifiedIntent，
     * 则跳过 LLM 分类走快速通道（由 InputGovernance.governWithHint 处理）。
     */
    public String executeSyncWithContext(QueryLoopContext ctx, String traceId) {
        long t0 = System.currentTimeMillis();
        log.info("[QL][{}] ══════ Plan Step START intent={} input=\"{}\" ══════",
                traceId, ctx.getMetadata().getOrDefault("planPreClassifiedIntent", "?"),
                truncate(ctx.getCleanedInput(), 60));

        // 2. 意图治理
        Boolean skipLLM = (Boolean) ctx.getMetadata().getOrDefault("skipLLMClassification", false);
        if (skipLLM && ctx.getMetadata().containsKey("planPreClassifiedIntent")) {
            String intentName = (String) ctx.getMetadata().get("planPreClassifiedIntent");
            IntentType preClassified;
            try {
                preClassified = IntentType.valueOf(intentName);
            } catch (IllegalArgumentException e) {
                preClassified = IntentType.GENERAL_CHAT;
            }
            inputGovernance.governWithHint(ctx, preClassified, traceId);
        } else {
            inputGovernance.govern(ctx, traceId);
        }

        // 3. 规划执行
        long t3 = System.currentTimeMillis();
        String response = planner.planAndExecute(ctx, traceId)
                .collectList()
                .map(list -> String.join("", list))
                .block();
        logStage(traceId, "3/5 Planner", t3);

        // ★ OutputAuditor：输出安全审计
        long ta = System.currentTimeMillis();
        OutputAuditor.AuditResult auditResult = outputAuditor.audit(
                response != null ? response : "", ctx.getUserId(), traceId);
        String auditedResponse = auditResult.getAuditedOutput();

        // 4. 状态持久化
        long t4 = System.currentTimeMillis();
        ctx.setResponse(auditedResponse);
        stateWriter.writeState(ctx, traceId);
        logStage(traceId, "4/5 StateWriter", t4);

        long total = System.currentTimeMillis() - t0;
        log.info("[QL][{}] ══════ Plan Step DONE totalMs={} responseLen={} ══════",
                traceId, total, ctx.getResponse().length());
        return ctx.getResponse();
    }

    // ═══════════════════════════════════════════════
    // Phase 2: 业务工具直接调用
    // ═══════════════════════════════════════════════

    /**
     * 执行已注册的业务工具（非 Function Calling 路径）
     */
    public Map<String, Object> executeBusinessTool(String msg, String uid, String toolName) {
        UserProfile profile = preProcessor.load(uid, "tool-" + newTraceId());
        if (!profile.canAccess(IntentType.TOOL_CALL)) {
            return Map.of("success", false, "error", "工具调用需要 PREMIUM 及以上权益（当前: " + profile.getTier() + "）");
        }

        Map<String, Object> params = new HashMap<>();
        params.put("message", msg);
        params.put("userId", uid);
        params.put("toolName", toolName);

        ToolResult toolResult = planner.executeBusinessTool(toolName, params);

        // 缓存到会话状态
        SessionState session = stateReader.getSessionState(uid);
        session.cacheToolResult(toolName, toolResult);
        try {
            stateWriter.writeSessionOnly(session);
        } catch (Exception e) {
            log.error("[QL] executeBusinessTool 状态缓存失败 uid={} err={}", uid, e.toString());
        }

        Map<String, Object> result = new HashMap<>();
        result.put("success", toolResult.isSuccess());
        result.put("toolName", toolResult.getToolName());
        result.put("summary", toolResult.getSummary());
        result.put("elapsedMs", toolResult.getElapsedMs());
        result.put("data", toolResult.getData());
        if (!toolResult.isSuccess()) {
            result.put("errorMessage", toolResult.getErrorMessage());
        }
        return result;
    }

    // ═══════════════════════════════════════════════
    // 管线装配 (PreProcessor → StateReader → InputGovernance)
    // ═══════════════════════════════════════════════
    private QueryLoopContext assemblePipeline(String rawInput, String userId,
                                               String traceId, long pipelineStartMs) {
        // 0. 加载用户权益
        long ta = System.currentTimeMillis();
        UserProfile profile = preProcessor.load(userId, traceId);
        logStage(traceId, "0/5 PreProcessor", ta);

        // 1. 构建上下文 + 读取会话状态
        long tb = System.currentTimeMillis();
        QueryLoopContext ctx = new QueryLoopContext()
                .setUserId(userId)
                .setOriginalInput(rawInput)
                .setUserProfile(profile);
        stateReader.readState(ctx, traceId);
        logStage(traceId, "1/5 StateReader", tb);

        // ★ 1.5/5 InputSanitizer：输入安全过滤
        long ts = System.currentTimeMillis();
        InputSanitizer.SanitizeResult sanitizeResult = inputSanitizer.sanitize(
                rawInput, userId, traceId);
        if (sanitizeResult.isBlocked()) {
            ctx.setCleanedInput("");
            ctx.setIntent(IntentType.GENERAL_CHAT)
               .setFinalRoute(IntentType.GENERAL_CHAT)
               .setConfidence(0.0)
               .setRejectReason(sanitizeResult.getReason());
            // 存入 metadata 供 Planner 生成拒绝响应
            ctx.getMetadata().put("sanitizerBlocked", true);
            ctx.getMetadata().put("sanitizerReason", sanitizeResult.getReason());
        } else {
            ctx.setOriginalInput(sanitizeResult.getSanitizedInput());
        }
        logStage(traceId, "1.5/5 InputSanitizer", ts);

        // 2. 输入治理 (ctx 中已有 UserProfile + SessionState)
        long tc = System.currentTimeMillis();
        inputGovernance.govern(ctx, traceId);
        logStage(traceId, "2/5 InputGovernance", tc);

        return ctx;
    }

    private String newTraceId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private void logStage(String traceId, String stage, long startMs) {
        long elapsed = System.currentTimeMillis() - startMs;
        log.info("[QL][{}]   ◀ {} ({}ms)", traceId, stage, elapsed);
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
