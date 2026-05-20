package com.queryloop.plan;

import com.queryloop.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class ExecuteOrchestrator {

    private final QueryLoopService queryLoopService;
    private final StateReader stateReader;
    private final StateWriter stateWriter;
    private final PreProcessor preProcessor;
    private final PlanReflector reflector;
    private final PlanGenerator planGenerator;
    private final Planner planner;
    private final ToolRegistry toolRegistry;
    private final PlanCheckpointManager checkpointManager;

    public ExecuteOrchestrator(QueryLoopService queryLoopService,
                               StateReader stateReader,
                               StateWriter stateWriter,
                               PreProcessor preProcessor,
                               PlanReflector reflector,
                               PlanGenerator planGenerator,
                               Planner planner,
                               ToolRegistry toolRegistry,
                               PlanCheckpointManager checkpointManager) {
        this.queryLoopService = queryLoopService;
        this.stateReader = stateReader;
        this.stateWriter = stateWriter;
        this.preProcessor = preProcessor;
        this.reflector = reflector;
        this.planGenerator = planGenerator;
        this.planner = planner;
        this.toolRegistry = toolRegistry;
        this.checkpointManager = checkpointManager;
    }

    /**
     * 流式执行 Plan-and-Execute。
     *
     * @return Flux<PlanEvent> — plan → step_start → step_complete → ... → complete
     */
    public Flux<PlanEvent> executeStream(ExecutionPlan plan, String userId, String traceId) {
        Sinks.Many<PlanEvent> sink = Sinks.many().unicast().onBackpressureBuffer();
        CancellationToken cancelToken = new CancellationToken(plan.getPlanId());

        // 加载共享上下文（仅一次）
        UserProfile profile = preProcessor.load(userId, traceId);
        QueryLoopContext sharedCtx = new QueryLoopContext()
                .setUserId(userId)
                .setUserProfile(profile);
        stateReader.readState(sharedCtx, traceId);

        // 发送 plan 事件
        sink.tryEmitNext(PlanEvent.plan(plan));

        // 异步执行步骤（带取消支持）
        Thread execThread = new Thread(() -> {
            try {
                executeSteps(plan, sharedCtx, userId, traceId, sink, cancelToken);
            } catch (PlanCancelledException e) {
                log.info("[PlanExec][{}] Plan 被取消 planId={}", traceId, plan.getPlanId());
                plan.setStatus(PlanStatus.FAILED);
                checkpointManager.savePlanSnapshot(plan, traceId);
                sink.tryEmitNext(PlanEvent.error("执行被取消"));
                sink.tryEmitComplete();
            } catch (Exception e) {
                log.error("[PlanExec][{}] 执行异常 err={}", traceId, e.toString());
                sink.tryEmitNext(PlanEvent.error(e.getMessage()));
                sink.tryEmitComplete();
            }
        }, "plan-exec-" + traceId);
        execThread.start();

        // doOnCancel：客户端断开时触发取消令牌 + 中断后台线程
        return sink.asFlux()
                .doOnCancel(() -> {
                    log.warn("[PlanExec][{}] SSE 连接断开，触发取消 planId={}", traceId, plan.getPlanId());
                    cancelToken.cancel();
                    execThread.interrupt();
                });
    }

    /**
     * 从 checkpoint 恢复执行。
     */
    public Flux<PlanEvent> executeStreamWithRecovery(ExecutionPlan plan, String userId,
                                                      String traceId,
                                                      PlanCheckpointManager.PlanRecoveryResult recovery) {
        Sinks.Many<PlanEvent> sink = Sinks.many().unicast().onBackpressureBuffer();
        CancellationToken cancelToken = new CancellationToken(plan.getPlanId());

        UserProfile profile = preProcessor.load(userId, traceId);
        QueryLoopContext sharedCtx = new QueryLoopContext()
                .setUserId(userId)
                .setUserProfile(profile);
        stateReader.readState(sharedCtx, traceId);

        // 重放已完成步骤的 SSE 事件，让前端重建 UI 状态
        if (recovery.isRecoverable()) {
            int resumeFrom = recovery.getResumeFromSeq();
            log.info("[PlanExec][{}] Checkpoint 恢复: 从 Step {} 继续", traceId, resumeFrom);
            for (PlanStep s : plan.getSteps()) {
                if (s.getSeq() < resumeFrom) {
                    if (s.getStatus() == StepStatus.COMPLETED) {
                        sink.tryEmitNext(PlanEvent.stepComplete(s.getSeq(), s));
                    } else if (s.getStatus() == StepStatus.FAILED) {
                        sink.tryEmitNext(PlanEvent.stepFailed(s.getSeq(), "前序执行失败"));
                    } else if (s.getStatus() == StepStatus.SKIPPED) {
                        sink.tryEmitNext(PlanEvent.stepSkipped(s.getSeq(), "重规划时跳过"));
                    }
                }
            }
        }

        sink.tryEmitNext(PlanEvent.plan(plan));

        Thread execThread = new Thread(() -> {
            try {
                executeSteps(plan, sharedCtx, userId, traceId, sink, cancelToken);
            } catch (PlanCancelledException e) {
                log.info("[PlanExec][{}] 恢复执行被取消 planId={}", traceId, plan.getPlanId());
                checkpointManager.savePlanSnapshot(plan, traceId);
                sink.tryEmitNext(PlanEvent.error("执行被取消"));
                sink.tryEmitComplete();
            } catch (Exception e) {
                log.error("[PlanExec][{}] 恢复执行异常 err={}", traceId, e.toString());
                sink.tryEmitNext(PlanEvent.error(e.getMessage()));
                sink.tryEmitComplete();
            }
        }, "plan-exec-" + traceId);
        execThread.start();

        return sink.asFlux()
                .doOnCancel(() -> {
                    cancelToken.cancel();
                    execThread.interrupt();
                });
    }

    /**
     * 同步执行 Plan-and-Execute，阻塞等待全部完成。
     */
    public String executeSync(ExecutionPlan plan, String userId, String traceId) {
        UserProfile profile = preProcessor.load(userId, traceId);
        QueryLoopContext sharedCtx = new QueryLoopContext()
                .setUserId(userId)
                .setUserProfile(profile);
        stateReader.readState(sharedCtx, traceId);

        CancellationToken cancelToken = new CancellationToken(plan.getPlanId());
        executeStepsBlocking(plan, sharedCtx, traceId, cancelToken);
        return buildSummary(plan);
    }

    // ═══════════════════════════════════════════════
    // 核心执行循环（流式）
    // ═══════════════════════════════════════════════

    private void executeSteps(ExecutionPlan plan, QueryLoopContext sharedCtx,
                              String userId, String traceId,
                              Sinks.Many<PlanEvent> sink,
                              CancellationToken cancelToken) {
        plan.setStatus(PlanStatus.EXECUTING);
        checkpointManager.savePlanSnapshot(plan, traceId);

        while (plan.hasNextStep()) {
            // ★ 每个 step 边界检查取消
            cancelToken.throwIfCancelled();

            int idx = plan.getCurrentStepIndex() + 1;
            PlanStep step = plan.getSteps().get(idx);

            // ★ 跳过已标记为 SKIPPED 的 step（replan 残留）
            if (step.getStatus() == StepStatus.SKIPPED || step.getStatus() == StepStatus.COMPLETED) {
                plan.setCurrentStepIndex(idx);
                if (step.getStatus() == StepStatus.SKIPPED) {
                    log.info("[PlanExec][{}] Step {} 已标记 SKIPPED，跳过", traceId, idx);
                }
                continue;
            }

            plan.setCurrentStepIndex(idx);

            // 检查依赖是否完成
            if (!dependenciesSatisfied(step, plan)) {
                step.setStatus(StepStatus.SKIPPED);
                log.info("[PlanExec][{}] Step {} 跳过：依赖步骤失败或未完成", traceId, idx);
                sink.tryEmitNext(PlanEvent.stepSkipped(idx, "依赖步骤失败或未完成"));
                checkpointManager.saveStepCheckpoint(plan.getPlanId(), step,
                        StepCheckpointType.AFTER_FAILURE, userId, traceId);
                continue;
            }

            // ★ Step 执行前 checkpoint
            checkpointManager.saveStepCheckpoint(plan.getPlanId(), step,
                    StepCheckpointType.BEFORE_EXECUTION, userId, traceId);

            // 执行单步
            executeStep(step, plan, sharedCtx, traceId, sink, cancelToken);

            // ★ Step 执行后 checkpoint
            StepCheckpointType ckType = step.getStatus() == StepStatus.COMPLETED
                    ? StepCheckpointType.AFTER_COMPLETION
                    : StepCheckpointType.AFTER_FAILURE;
            checkpointManager.saveStepCheckpoint(plan.getPlanId(), step, ckType, userId, traceId);
            checkpointManager.savePlanSnapshot(plan, traceId);

            // 每步后 Reflection
            if (plan.hasNextStep()) {
                cancelToken.throwIfCancelled();

                ReflectionResult reflection = reflector.reflect(plan, sharedCtx, traceId);

                if (reflection.getAction() == ReflectionAction.REPLAN && plan.canReplan()) {
                    log.info("[PlanExec][{}] Reflection 触发重规划 reason={}", traceId, reflection.getReason());
                    plan.setReplanCount(plan.getReplanCount() + 1);

                    // 丢弃剩余步骤
                    List<PlanStep> remaining = new ArrayList<>(
                            plan.getSteps().subList(idx + 1, plan.getSteps().size()));
                    for (PlanStep s : remaining) {
                        s.setStatus(StepStatus.SKIPPED);
                    }

                    // 重新生成步骤
                    ExecutionPlan newPlan = planGenerator.generate(
                            plan.getOriginalQuery(),
                            sharedCtx.getUserProfile().toPromptContext());

                    int nextSeq = idx + 1;
                    for (PlanStep ns : newPlan.getSteps()) {
                        ns.setStepId(java.util.UUID.randomUUID().toString().substring(0, 8));
                        ns.setSeq(nextSeq++);
                        plan.getSteps().add(ns);
                    }

                    // ★ replan 后 checkpoint
                    checkpointManager.savePlanSnapshot(plan, traceId);
                    sink.tryEmitNext(PlanEvent.replan(plan, reflection.getReason()));

                } else if (reflection.getAction() == ReflectionAction.ABORT) {
                    log.warn("[PlanExec][{}] Reflection 中止执行 reason={}", traceId, reflection.getReason());
                    plan.setStatus(PlanStatus.FAILED);
                    checkpointManager.savePlanSnapshot(plan, traceId);
                    sink.tryEmitNext(PlanEvent.error("执行中止: " + reflection.getReason()));
                    sink.tryEmitComplete();
                    return;
                }
                // CONTINUE → 继续下一步
            }
        }

        // 全部完成
        plan.setStatus(PlanStatus.COMPLETED);
        String summary = buildSummary(plan);
        checkpointManager.savePlanSnapshot(plan, traceId);
        sink.tryEmitNext(PlanEvent.complete(summary));
        sink.tryEmitComplete();
        log.info("[PlanExec][{}] ══════ 执行完成 {} steps ══════", traceId, plan.getSteps().size());
    }

    /**
     * 同步执行循环（无 Sink）
     */
    private void executeStepsBlocking(ExecutionPlan plan, QueryLoopContext sharedCtx,
                                       String traceId, CancellationToken cancelToken) {
        plan.setStatus(PlanStatus.EXECUTING);

        while (plan.hasNextStep()) {
            cancelToken.throwIfCancelled();

            int idx = plan.getCurrentStepIndex() + 1;
            PlanStep step = plan.getSteps().get(idx);

            if (step.getStatus() == StepStatus.SKIPPED || step.getStatus() == StepStatus.COMPLETED) {
                plan.setCurrentStepIndex(idx);
                continue;
            }

            plan.setCurrentStepIndex(idx);

            if (!dependenciesSatisfied(step, plan)) {
                step.setStatus(StepStatus.SKIPPED);
                continue;
            }

            executeStepBlocking(step, plan, sharedCtx, traceId, cancelToken);

            if (plan.hasNextStep()) {
                cancelToken.throwIfCancelled();

                ReflectionResult reflection = reflector.reflect(plan, sharedCtx, traceId);

                if (reflection.getAction() == ReflectionAction.REPLAN && plan.canReplan()) {
                    plan.setReplanCount(plan.getReplanCount() + 1);
                    List<PlanStep> remaining = new ArrayList<>(
                            plan.getSteps().subList(idx + 1, plan.getSteps().size()));
                    for (PlanStep s : remaining) s.setStatus(StepStatus.SKIPPED);

                    ExecutionPlan newPlan = planGenerator.generate(
                            plan.getOriginalQuery(),
                            sharedCtx.getUserProfile().toPromptContext());

                    int nextSeq = idx + 1;
                    for (PlanStep ns : newPlan.getSteps()) {
                        ns.setStepId(java.util.UUID.randomUUID().toString().substring(0, 8));
                        ns.setSeq(nextSeq++);
                        plan.getSteps().add(ns);
                    }
                } else if (reflection.getAction() == ReflectionAction.ABORT) {
                    plan.setStatus(PlanStatus.FAILED);
                    return;
                }
            }
        }
        plan.setStatus(PlanStatus.COMPLETED);
    }

    // ═══════════════════════════════════════════════
    // 单步执行
    // ═══════════════════════════════════════════════

    private void executeStep(PlanStep step, ExecutionPlan plan,
                             QueryLoopContext sharedCtx, String traceId,
                             Sinks.Many<PlanEvent> sink,
                             CancellationToken cancelToken) {
        cancelToken.throwIfCancelled();

        step.setStatus(StepStatus.RUNNING);
        step.setStartedAt(System.currentTimeMillis());
        sink.tryEmitNext(PlanEvent.stepStart(step.getSeq(), step));

        String enrichedQuery = enrichQuery(step, plan);
        String stepResult;
        try {
            stepResult = executeSingleStep(enrichedQuery, step.getIntent(), sharedCtx, traceId, cancelToken);
            step.setResult(stepResult);
            step.setStatus(StepStatus.COMPLETED);
            step.setCompletedAt(System.currentTimeMillis());
            sink.tryEmitNext(PlanEvent.stepComplete(step.getSeq(), step));
            log.info("[PlanExec][{}] Step {} 完成 intent={} resultLen={}",
                    traceId, step.getSeq(), step.getIntent(), stepResult != null ? stepResult.length() : 0);
        } catch (PlanCancelledException e) {
            throw e; // 重新抛出，让上层处理
        } catch (Exception e) {
            log.error("[PlanExec][{}] Step {} 执行失败 err={}", traceId, step.getSeq(), e.toString());
            step.setStatus(StepStatus.FAILED);
            step.setCompletedAt(System.currentTimeMillis());
            sink.tryEmitNext(PlanEvent.stepFailed(step.getSeq(), e.getMessage()));
        }
    }

    private void executeStepBlocking(PlanStep step, ExecutionPlan plan,
                                     QueryLoopContext sharedCtx, String traceId,
                                     CancellationToken cancelToken) {
        cancelToken.throwIfCancelled();

        step.setStatus(StepStatus.RUNNING);
        step.setStartedAt(System.currentTimeMillis());

        String enrichedQuery = enrichQuery(step, plan);
        String stepResult;
        try {
            stepResult = executeSingleStep(enrichedQuery, step.getIntent(), sharedCtx, traceId, cancelToken);
            step.setResult(stepResult);
            step.setStatus(StepStatus.COMPLETED);
            step.setCompletedAt(System.currentTimeMillis());
            log.info("[PlanExec][{}] Step {} 完成 intent={} resultLen={}",
                    traceId, step.getSeq(), step.getIntent(), stepResult != null ? stepResult.length() : 0);
        } catch (PlanCancelledException e) {
            throw e;
        } catch (Exception e) {
            log.error("[PlanExec][{}] Step {} 执行失败 err={}", traceId, step.getSeq(), e.toString());
            step.setStatus(StepStatus.FAILED);
        }
    }

    /**
     * 执行单步 —— Plan-and-Execute 与 Query Loop 的桥接点。
     *
     * 策略：根据 plan 中预分类的 intent，两条路径：
     *   Path A (TOOL_CALL) → 直接 executeBusinessTool + 手动写 StateWriter
     *   Path B (其他) → 走完整 Query Loop Pipeline (executeSyncWithContext)
     */
    private String executeSingleStep(String enrichedQuery, IntentType intent,
                                      QueryLoopContext sharedCtx, String traceId,
                                      CancellationToken cancelToken) {
        cancelToken.throwIfCancelled();

        // 更新 sharedCtx 为当前 step 的子查询
        sharedCtx.setOriginalInput(enrichedQuery);
        sharedCtx.setCleanedInput(enrichedQuery.strip().replaceAll("\\s{2,}", " "));

        // 设置 plan 预分类元数据 → InputGovernance 可跳过 LLM 分类
        sharedCtx.getMetadata().put("planPreClassifiedIntent", intent.name());
        sharedCtx.getMetadata().put("skipLLMClassification", true);

        // Path A: TOOL_CALL — 尝试匹配已注册工具直接调用
        if (intent == IntentType.TOOL_CALL) {
            cancelToken.throwIfCancelled();
            String toolResult = executeAsBusinessTool(enrichedQuery, sharedCtx, traceId);
            if (toolResult != null) return toolResult;
            // 未匹配到具体工具 → 回退到 Function Calling 路径
        }

        // Path B: 走完整 Query Loop Pipeline
        cancelToken.throwIfCancelled();
        return queryLoopService.executeSyncWithContext(sharedCtx, traceId);
    }

    /**
     * 尝试将 step 作为业务工具直接执行（跳过 LLM 分类）。
     * 返回 null 表示未匹配到工具，需要回退到完整 Pipeline。
     */
    private String executeAsBusinessTool(String query, QueryLoopContext sharedCtx, String traceId) {
        if (query.contains("ORD") || query.contains("订单")) {
            String orderId = extractParam(query, "ORD\\d+", "ORD001");
            ToolResult result = planner.executeBusinessTool("orderQuery",
                    java.util.Map.of("orderId", orderId));
            return formatToolResult(result, sharedCtx, "orderQuery");
        }

        if (query.contains("TN") || query.contains("物流") || query.contains("单号")) {
            String trackingNo = extractParam(query, "TN\\d+", "TN001");
            ToolResult result = planner.executeBusinessTool("logisticsTrace",
                    java.util.Map.of("trackingNumber", trackingNo));
            return formatToolResult(result, sharedCtx, "logisticsTrace");
        }

        if (query.contains("工单") || query.contains("创建")) {
            String patientName = extractName(query);
            ToolResult result = planner.executeBusinessTool("workOrderCreate",
                    java.util.Map.of("patientName", patientName, "serviceType", "护理服务"));
            return formatToolResult(result, sharedCtx, "workOrderCreate");
        }

        return null;
    }

    private String extractParam(String text, String regex, String defaultVal) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(regex).matcher(text);
        return m.find() ? m.group() : defaultVal;
    }

    private String formatToolResult(ToolResult result, QueryLoopContext sharedCtx, String toolName) {
        sharedCtx.getSessionState().cacheToolResult(toolName, result);
        try {
            stateWriter.writeSessionOnly(sharedCtx.getSessionState());
        } catch (Exception ignored) {}

        if (result.isSuccess()) {
            return result.getSummary() + "\n" + result.getData().toString();
        }
        return result.getErrorMessage() != null ? result.getErrorMessage() : "工具执行失败";
    }

    private String extractName(String text) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("[\\u4e00-\\u9fa5]{2,3}(?:老人|奶奶|爷爷|先生|女士)?")
                .matcher(text);
        return m.find() ? m.group().replaceAll("(?:老人|奶奶|爷爷|先生|女士)", "") : "未指定";
    }

    // ═══════════════════════════════════════════════
    // 辅助方法
    // ═══════════════════════════════════════════════

    private boolean dependenciesSatisfied(PlanStep step, ExecutionPlan plan) {
        if (step.getDependsOn() == null || step.getDependsOn().isEmpty()) return true;
        for (int depSeq : step.getDependsOn()) {
            PlanStep dep = plan.getSteps().stream()
                    .filter(s -> s.getSeq() == depSeq).findFirst().orElse(null);
            if (dep == null || dep.getStatus() != StepStatus.COMPLETED) return false;
        }
        return true;
    }

    /**
     * 注入前序步骤结果到当前 subQuery。
     */
    String enrichQuery(PlanStep step, ExecutionPlan plan) {
        String base = step.getSubQuery();
        String context = plan.getCompletedContext();
        if (context.isEmpty()) return base;

        if (step.getDependsOn() != null && !step.getDependsOn().isEmpty()) {
            return String.format("【前序步骤结果】\n%s\n【当前任务】%s", context, base);
        }
        return base;
    }

    private String buildSummary(ExecutionPlan plan) {
        StringBuilder sb = new StringBuilder();
        for (PlanStep step : plan.getSteps()) {
            if (step.getStatus() == StepStatus.COMPLETED && step.getResult() != null) {
                if (sb.length() > 0) sb.append("\n\n");
                sb.append(step.getResult());
            }
        }
        return sb.toString();
    }
}
