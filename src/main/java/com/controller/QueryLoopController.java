package com.controller;

import com.queryloop.*;
import com.queryloop.plan.PlanEvent;
import com.queryloop.plan.PlanExecuteService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.Map;

@Slf4j
@RestController
public class QueryLoopController {

    @Resource
    private QueryLoopService queryLoopService;

    @Resource
    private PlanExecuteService planExecuteService;

    @Resource
    private QueryLoopOrchestrator queryLoopOrchestrator;

    // orchestrate* 端点通过 QueryLoopService 执行（含 StateWriter），
    // planExecute* 端点通过 PlanExecuteService 执行（原有 Plan-and-Execute）

    /**
     * 流式 Query Loop — SSE 实时输出
     */
    @GetMapping(value = "/ai/query-loop", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> queryLoop(
            @RequestParam("msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[QL-Ctrl] → SSE stream request uid={} msg=\"{}\"", uid, truncate(msg, 60));
        return queryLoopService.execute(msg, uid)
                .doOnComplete(() -> log.info("[QL-Ctrl] ← SSE stream finished uid={}", uid))
                .doOnCancel(() -> log.warn("[QL-Ctrl] ← SSE stream cancelled uid={}", uid))
                .doOnError(e -> log.error("[QL-Ctrl] ← SSE stream error uid={} err={}", uid, e.toString()));
    }

    /**
     * 同步 Query Loop — 阻塞等待完整响应
     */
    @GetMapping("/ai/query-loop/sync")
    public String queryLoopSync(
            @RequestParam("msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[QL-Ctrl] → sync request uid={} msg=\"{}\"", uid, truncate(msg, 60));
        long t0 = System.currentTimeMillis();
        String result = queryLoopService.executeSync(msg, uid);
        long elapsed = System.currentTimeMillis() - t0;
        log.info("[QL-Ctrl] ← sync response uid={} totalMs={} responseLen={}",
                uid, elapsed, result != null ? result.length() : 0);
        return result;
    }

    // ═══════════════════════════════════════════════
    // Phase 1: 意图反馈接口
    // ═══════════════════════════════════════════════

    /**
     * 用户纠正意图分类结果
     * POST /ai/query-loop/feedback?uid=xxx&correctIntent=TOOL_CALL
     */
    @PostMapping("/ai/query-loop/feedback")
    public Map<String, Object> submitFeedback(
            @RequestParam("uid") String uid,
            @RequestParam("correctIntent") String correctIntentName) {

        IntentType correctIntent;
        try {
            correctIntent = IntentType.valueOf(correctIntentName.toUpperCase());
        } catch (IllegalArgumentException e) {
            return Map.of("success", false, "error", "无效的意图类型: " + correctIntentName);
        }

        Map<String, Object> stats = queryLoopService.recordCorrection(uid, correctIntent);
        log.info("[QL-Ctrl] ← feedback uid={} correctIntent={} stats={}", uid, correctIntent, stats);
        return Map.of("success", true, "stats", stats);
    }

    /**
     * 查询当前会话的意图分类准确率统计
     * GET /ai/query-loop/stats?uid=xxx
     */
    @GetMapping("/ai/query-loop/stats")
    public Map<String, Object> getFeedbackStats(@RequestParam("uid") String uid) {
        Map<String, Object> stats = queryLoopService.getFeedbackStats(uid);
        log.info("[QL-Ctrl] ← stats uid={} stats={}", uid, stats);
        return stats;
    }

    /**
     * Phase 2: 直接调用业务工具接口
     * GET /ai/query-loop/tool?msg=查询订单ORD001&uid=xxx&tool=orderQuery
     */
    @GetMapping("/ai/query-loop/tool")
    public Map<String, Object> callBusinessTool(
            @RequestParam("msg") String msg,
            @RequestParam("uid") String uid,
            @RequestParam("tool") String toolName) {

        log.info("[QL-Ctrl] → tool call uid={} tool={} msg=\"{}\"", uid, toolName, truncate(msg, 60));
        Map<String, Object> result = queryLoopService.executeBusinessTool(msg, uid, toolName);
        log.info("[QL-Ctrl] ← tool result uid={} tool={} success={}",
                uid, toolName, result.get("success"));
        return result;
    }

    // ═══════════════════════════════════════════════
    // Phase 4: Plan-and-Execute 接口
    // ═══════════════════════════════════════════════

    /**
     * Plan-and-Execute 流式接口。
     *
     * SSE 事件流格式：
     *   event: plan          data: {"type":"plan","totalSteps":3,...}
     *   event: step_start    data: {"type":"step_start","stepIndex":0,"intent":"TOOL_CALL",...}
     *   event: step_complete data: {"type":"step_complete","stepIndex":0,"result":"...",...}
     *   event: replan        data: {"type":"replan","reason":"...",...}
     *   event: complete      data: {"type":"complete","summary":"..."}
     *   event: error         data: {"type":"error","message":"..."}
     *
     * 前端使用 EventSource API 消费：
     *   const es = new EventSource('/ai/query-loop/plan-execute/stream?msg=...&uid=...');
     *   es.addEventListener('plan', e => renderPlan(JSON.parse(e.data)));
     *   es.addEventListener('step_complete', e => appendResult(JSON.parse(e.data)));
     *   es.addEventListener('complete', e => showSummary(JSON.parse(e.data)));
     */
    @GetMapping(value = "/ai/query-loop/plan-execute/stream",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<PlanEvent>> planExecuteStream(
            @RequestParam("msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[PE-Ctrl] → Plan-Execute SSE uid={} msg=\"{}\"", uid, truncate(msg, 60));
        return planExecuteService.executeStream(msg, uid)
                .map(event -> ServerSentEvent.<PlanEvent>builder()
                        .event(event.getType())
                        .data(event)
                        .build())
                .doOnComplete(() -> log.info("[PE-Ctrl] ← Plan-Execute SSE finished uid={}", uid))
                .doOnCancel(() -> log.warn("[PE-Ctrl] ← Plan-Execute SSE cancelled uid={}", uid))
                .doOnError(e -> log.error("[PE-Ctrl] ← Plan-Execute SSE err={}", e.toString()));
    }

    /**
     * Plan-and-Execute 恢复接口。
     * 客户端断线重连时，传入上次的 planId 从断点继续接收 SSE 事件。
     *
     * 前端重连示例：
     *   const es = new EventSource('/ai/query-loop/plan-execute/resume?planId=abc123&uid=user1');
     */
    @GetMapping(value = "/ai/query-loop/plan-execute/resume",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<PlanEvent>> planExecuteResume(
            @RequestParam("planId") String planId,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[PE-Ctrl] → Plan-Execute RESUME planId={} uid={}", planId, uid);
        return planExecuteService.resumeStream(planId, uid)
                .map(event -> ServerSentEvent.<PlanEvent>builder()
                        .event(event.getType())
                        .data(event)
                        .build())
                .doOnComplete(() -> log.info("[PE-Ctrl] ← Plan-Execute RESUME finished planId={}", planId))
                .doOnCancel(() -> log.warn("[PE-Ctrl] ← Plan-Execute RESUME cancelled planId={}", planId))
                .doOnError(e -> log.error("[PE-Ctrl] ← Plan-Execute RESUME err={}", e.toString()));
    }

    /**
     * Plan-and-Execute 同步接口。
     * 阻塞等待全部步骤执行完毕，返回完整结果 JSON。
     */
    @GetMapping("/ai/query-loop/plan-execute/sync")
    public Map<String, Object> planExecuteSync(
            @RequestParam("msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[PE-Ctrl] → Plan-Execute sync uid={} msg=\"{}\"", uid, truncate(msg, 60));
        long t0 = System.currentTimeMillis();

        Map<String, Object> result = planExecuteService.executeSync(msg, uid);

        long elapsed = System.currentTimeMillis() - t0;
        log.info("[PE-Ctrl] ← Plan-Execute sync uid={} totalMs={}", uid, elapsed);
        return result;
    }

    // ═══════════════════════════════════════════════
    // Phase 6: Router → Plan → Worker 三层编排接口
    // ═══════════════════════════════════════════════

    /**
     * Router → Plan → Worker 流式接口。
     *
     * <p>与 /plan-execute/stream 的区别：
     * <ul>
     *   <li>本接口自动完成意图分类（Router）→ 计划生成（Planner）→ 步骤执行（Worker）</li>
     *   <li>用户画像在 Router 和 Planner 之前加载，确保分类和规划阶段已有权益信息</li>
     *   <li>简单查询（GENERAL_CHAT / MEMORY_QUERY）自动跳过 Planner，直接 Worker 单步执行</li>
     * </ul>
     *
     * <p>SSE 事件流：
     *   event: plan        → {"type":"plan","totalSteps":3,...}
     *   event: step_start  → {"type":"step_start","stepIndex":0,...}
     *   event: step_complete → {"type":"step_complete",...}
     *   event: complete    → {"type":"complete","summary":"..."}
     */
    @GetMapping(value = "/ai/query-loop/orchestrate/stream",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<PlanEvent>> orchestrateStream(
            @RequestParam("msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[QLO-Ctrl] → Router→Plan→Worker SSE uid={} msg=\"{}\"", uid, truncate(msg, 60));
        return queryLoopService.executeOrchestrated(msg, uid)
                .map(event -> ServerSentEvent.<PlanEvent>builder()
                        .event(event.getType())
                        .data(event)
                        .build())
                .doOnComplete(() -> log.info("[QLO-Ctrl] ← Router→Plan→Worker SSE finished uid={}", uid))
                .doOnCancel(() -> log.warn("[QLO-Ctrl] ← Router→Plan→Worker SSE cancelled uid={}", uid))
                .doOnError(e -> log.error("[QLO-Ctrl] ← Router→Plan→Worker SSE err={}", e.toString()));
    }

    /**
     * Router → Plan → Worker 同步接口。
     * 阻塞等待全部步骤执行完毕，返回完整结果 JSON。
     */
    @GetMapping("/ai/query-loop/orchestrate/sync")
    public Map<String, Object> orchestrateSync(
            @RequestParam("msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[QLO-Ctrl] → Router→Plan→Worker sync uid={} msg=\"{}\"", uid, truncate(msg, 60));
        long t0 = System.currentTimeMillis();

        Map<String, Object> result = queryLoopService.executeOrchestratedSync(msg, uid);

        long elapsed = System.currentTimeMillis() - t0;
        log.info("[QLO-Ctrl] ← Router→Plan→Worker sync uid={} totalMs={}", uid, elapsed);
        return result;
    }

    /**
     * Router → Plan → Worker 恢复接口。
     * 客户端断线重连时，传入 planId 从断点继续。
     */
    @GetMapping(value = "/ai/query-loop/orchestrate/resume",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<PlanEvent>> orchestrateResume(
            @RequestParam("planId") String planId,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {

        log.info("[QLO-Ctrl] → Router→Plan→Worker RESUME planId={} uid={}", planId, uid);
        return queryLoopService.resumeOrchestrated(planId, uid)
                .map(event -> ServerSentEvent.<PlanEvent>builder()
                        .event(event.getType())
                        .data(event)
                        .build())
                .doOnComplete(() -> log.info("[QLO-Ctrl] ← Router→Plan→Worker RESUME finished planId={}", planId))
                .doOnCancel(() -> log.warn("[QLO-Ctrl] ← Router→Plan→Worker RESUME cancelled planId={}", planId))
                .doOnError(e -> log.error("[QLO-Ctrl] ← Router→Plan→Worker RESUME err={}", e.toString()));
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
