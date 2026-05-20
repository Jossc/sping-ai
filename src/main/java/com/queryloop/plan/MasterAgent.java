package com.queryloop.plan;

import com.queryloop.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * 统一编排器，整合 {@link PlanCheckpointManager} 的检查点能力与
 * {@link CancellationToken} 的协作式中断能力，提供可优雅停止的 Plan 执行流程。
 *
 * <h3>状态机</h3>
 * <pre>
 *   IDLE → RUNNING → COMPLETED
 *                   → CANCELLING → CANCELLED
 *                   → FAILED
 * </pre>
 *
 * <h3>两种停止模式</h3>
 * <table>
 *   <tr><th>模式</th><th>方法</th><th>行为</th></tr>
 *   <tr><td>优雅停止</td><td>{@link #cancelGracefully()}</td><td>等当前 Step 完成后停止，Checkpoint 完整保存</td></tr>
 *   <tr><td>立即停止</td><td>{@link #cancelImmediately()}</td><td>中断当前 Step，尽可能保存 Checkpoint</td></tr>
 * </table>
 *
 * <h3>恢复机制</h3>
 * 停机后调用 {@link #resume(String, String)} 从 Redis Checkpoint 恢复，
 * 重放已完成 Step 的 SSE 事件，从断点继续执行。
 */
@Slf4j
@Component
public class MasterAgent {

    private final PlanCheckpointManager checkpointManager;
    private final QueryLoopService queryLoopService;
    private final StateReader stateReader;
    private final StateWriter stateWriter;
    private final PreProcessor preProcessor;
    private final PlanReflector reflector;
    private final PlanGenerator planGenerator;
    private final Planner planner;
    private final StateBus stateBus;

    /** 当前生命周期状态 */
    private final AtomicReference<AgentState> state = new AtomicReference<>(AgentState.IDLE);

    /** 当前执行中的取消令牌 */
    private volatile CancellationToken activeCancelToken;

    /** 当前后台执行线程 */
    private volatile Thread activeThread;

    /** 当前 SSE 事件出口 */
    private volatile Sinks.Many<PlanEvent> activeSink;

    /** 当前执行计划 */
    private volatile ExecutionPlan activePlan;

    /** 工具参数提取正则 */
    private static final Pattern ORDER_ID_PATTERN = Pattern.compile("ORD\\d+");
    private static final Pattern TRACKING_PATTERN = Pattern.compile("TN\\d+");
    private static final Pattern NAME_PATTERN =
            Pattern.compile("[\\u4e00-\\u9fa5]{2,3}(?:老人|奶奶|爷爷|先生|女士)?");

    // ═══════════════════════════════════════════════════════════
    // 超时控制
    // ═══════════════════════════════════════════════════════════

    /**
     * 超时配置，构造时可自定义，未指定则使用默认值。
     */
    public record PlanTimeoutConfig(
            /** 单个 Step 执行超时（默认 60s） */
            Duration stepTimeout,
            /** 整个 Plan 执行超时（默认 5min） */
            Duration planTimeout,
            /** Reflection LLM 调用超时（默认 15s） */
            Duration reflectionTimeout,
            /** Replan LLM 调用超时（默认 30s） */
            Duration replanTimeout,
            /** 基础设施调用超时，Redis 等（默认 5s） */
            Duration infraTimeout
    ) {
        public static PlanTimeoutConfig defaults() {
            return new PlanTimeoutConfig(
                    Duration.ofSeconds(60),
                    Duration.ofMinutes(5),
                    Duration.ofSeconds(15),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5)
            );
        }
    }

    /** 单线程调度器，所有超时定时器共用（daemon 线程，不阻止 JVM 退出） */
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "master-agent-timeout");
                t.setDaemon(true);
                return t;
            });

    /** 当前生效的超时配置 */
    private volatile PlanTimeoutConfig timeoutConfig = PlanTimeoutConfig.defaults();

    /** Plan 级超时 Future（用于在正常完成时取消定时器） */
    private volatile ScheduledFuture<?> planTimeoutFuture;

    /** Step 级超时 Future（每个 Step 独立，完成时取消） */
    private volatile ScheduledFuture<?> stepTimeoutFuture;

    // ═══════════════════════════════════════════════════════════
    // 生命周期状态枚举
    // ═══════════════════════════════════════════════════════════

    public enum AgentState {
        /** 未启动 */
        IDLE,
        /** 执行中 */
        RUNNING,
        /** 优雅停止中：等待当前 Step 完成 */
        CANCELLING,
        /** 已取消 */
        CANCELLED,
        /** 全部步骤执行完成 */
        COMPLETED,
        /** 异常终止 */
        FAILED
    }

    // ═══════════════════════════════════════════════════════════
    // 构造
    // ═══════════════════════════════════════════════════════════

    public MasterAgent(PlanCheckpointManager checkpointManager,
                       QueryLoopService queryLoopService,
                       StateReader stateReader,
                       StateWriter stateWriter,
                       PreProcessor preProcessor,
                       PlanReflector reflector,
                       PlanGenerator planGenerator,
                       Planner planner,
                       StateBus stateBus) {
        this.checkpointManager = checkpointManager;
        this.queryLoopService = queryLoopService;
        this.stateReader = stateReader;
        this.stateWriter = stateWriter;
        this.preProcessor = preProcessor;
        this.reflector = reflector;
        this.planGenerator = planGenerator;
        this.planner = planner;
        this.stateBus = stateBus;
    }

    // ═══════════════════════════════════════════════════════════
    // 公开 API
    // ═══════════════════════════════════════════════════════════

    /**
     * 启动 Plan 流式执行。
     *
     * @param plan    已生成的执行计划
     * @param userId  用户 ID
     * @param traceId 调用链追踪 ID
     * @return SSE 事件流
     * @throws IllegalStateException 如果 Agent 当前不在 IDLE 状态
     */
    public Flux<PlanEvent> execute(ExecutionPlan plan, String userId, String traceId) {
        if (!state.compareAndSet(AgentState.IDLE, AgentState.RUNNING)) {
            return Flux.error(new IllegalStateException(
                    "MasterAgent 无法启动，当前状态: " + state.get() + "（需为 IDLE）"));
        }

        this.activePlan = plan;
        this.activeCancelToken = new CancellationToken(plan.getPlanId());
        this.activeSink = Sinks.many().unicast().onBackpressureBuffer();

        // 加载共享上下文
        UserProfile profile = preProcessor.load(userId, traceId);
        QueryLoopContext sharedCtx = new QueryLoopContext()
                .setUserId(userId)
                .setUserProfile(profile);
        stateReader.readState(sharedCtx, traceId);

        // ★ Checkpoint：Plan 执行前保存初始快照
        checkpointManager.savePlanSnapshot(plan, traceId);

        // 发送 plan 事件
        activeSink.tryEmitNext(PlanEvent.plan(plan));

        // ★ Plan 级超时：启动定时器，超时后自动触发立即停止
        schedulePlanTimeout(timeoutConfig.planTimeout(), traceId);

        // 启动后台执行线程
        this.activeThread = new Thread(() -> {
            try {
                executeSteps(plan, sharedCtx, traceId);
            } catch (PlanCancelledException e) {
                handleImmediateCancel(traceId, e);
            } catch (Exception e) {
                handleFailure(traceId, e);
            } finally {
                cancelStepTimeout();
                cancelPlanTimeout();
            }
        }, "master-agent-" + traceId);
        activeThread.start();

        log.info("[MasterAgent][{}] 执行已启动 planId={} steps={} confidence={}",
                traceId, plan.getPlanId(), plan.getSteps().size(), plan.getPlanConfidence());

        // SSE 连接断开时默认触发立即停止
        return activeSink.asFlux()
                .doOnCancel(() -> {
                    log.info("[MasterAgent][{}] SSE 连接断开，触发立即停止", traceId);
                    cancelImmediately();
                });
    }

    /**
     * 优雅停止：允许当前正在执行的 Step 完成后停止。
     *
     * <p>与 {@link #cancelImmediately()} 的区别：
     * <ul>
     *   <li>当前 Step 不会被中断，会正常执行完毕</li>
     *   <li>Step 完成后保存完整 Checkpoint（Plan + Step）</li>
     *   <li>后续 Step 不再执行</li>
     *   <li>SSE 流正常发送 error 事件后 complete</li>
     * </ul>
     *
     * @return true 如果成功触发优雅停止，false 如果状态不允许
     */
    public boolean cancelGracefully() {
        AgentState current = state.get();
        if (current != AgentState.RUNNING) {
            log.warn("[MasterAgent] 无法优雅停止，当前状态: {}（需为 RUNNING）", current);
            return false;
        }
        if (state.compareAndSet(AgentState.RUNNING, AgentState.CANCELLING)) {
            log.info("[MasterAgent] 优雅停止已触发 planId={}，当前 Step 完成后将停止",
                    activePlan != null ? activePlan.getPlanId() : "null");
            return true;
        }
        return false;
    }

    /**
     * 立即停止：中断当前 Step 和后台线程，尽可能保存 Checkpoint。
     *
     * <p>与 {@link #cancelGracefully()} 的区别：
     * <ul>
     *   <li>会中断正在执行的 Step（通过 CancellationToken + Thread.interrupt）</li>
     *   <li>Checkpoint 保存当前已完成的 Step 状态</li>
     *   <li>当前 Step 标记为 FAILED 或保持 RUNNING（恢复时重置）</li>
     * </ul>
     *
     * @return true 如果成功触发停止
     */
    public boolean cancelImmediately() {
        AgentState current = state.get();
        if (current != AgentState.RUNNING && current != AgentState.CANCELLING) {
            log.debug("[MasterAgent] 无需停止，当前状态: {}", current);
            return false;
        }

        log.info("[MasterAgent] 立即停止已触发 planId={}",
                activePlan != null ? activePlan.getPlanId() : "null");

        // 1. 触发取消令牌（原子标记 + 执行钩子）
        if (activeCancelToken != null) {
            activeCancelToken.cancel();
        }

        // 2. 中断后台线程
        if (activeThread != null && activeThread.isAlive()) {
            activeThread.interrupt();
        }

        return true;
    }

    /**
     * 从 Checkpoint 恢复执行。
     *
     * <p>恢复流程：
     * <ol>
     *   <li>从 Redis 读取 Plan 级 checkpoint</li>
     *   <li>重放已完成 Step 的 SSE 事件（前端重建 UI）</li>
     *   <li>发送 plan 事件</li>
     *   <li>从断点继续执行剩余 Step</li>
     * </ol>
     *
     * @param planId 要恢复的计划 ID
     * @param userId 用户 ID
     * @return SSE 事件流
     * @throws IllegalStateException 如果 Agent 不在 IDLE 或 checkpoint 不可恢复
     */
    public Flux<PlanEvent> resume(String planId, String userId) {
        if (!state.compareAndSet(AgentState.IDLE, AgentState.RUNNING)) {
            return Flux.error(new IllegalStateException(
                    "MasterAgent 无法恢复，当前状态: " + state.get() + "（需为 IDLE）"));
        }

        // 尝试恢复
        PlanCheckpointManager.PlanRecoveryResult recovery = checkpointManager.tryRecover(planId);
        if (!recovery.isRecoverable()) {
            state.set(AgentState.IDLE);
            return Flux.error(new IllegalStateException(
                    "Plan " + planId + " 无法恢复：checkpoint 不存在或已执行完成"));
        }

        this.activePlan = recovery.getPlan();
        this.activeCancelToken = new CancellationToken(planId);
        this.activeSink = Sinks.many().unicast().onBackpressureBuffer();

        String traceId = UUID.randomUUID().toString().substring(0, 8);
        int resumeFrom = recovery.getResumeFromSeq();

        log.info("[MasterAgent][{}] 恢复执行 planId={} fromStep={} totalSteps={}",
                traceId, planId, resumeFrom, activePlan.getSteps().size());

        // 加载上下文
        UserProfile profile = preProcessor.load(userId, traceId);
        QueryLoopContext sharedCtx = new QueryLoopContext()
                .setUserId(userId)
                .setUserProfile(profile);
        stateReader.readState(sharedCtx, traceId);

        // ★ 重放已完成 Step 的 SSE 事件，让前端重建 UI
        for (PlanStep s : activePlan.getSteps()) {
            if (s.getSeq() < resumeFrom) {
                replayStepEvent(s, activeSink, traceId);
            }
        }

        // 发送 plan 事件
        activeSink.tryEmitNext(PlanEvent.plan(activePlan));

        // ★ Plan 级超时
        schedulePlanTimeout(timeoutConfig.planTimeout(), traceId);

        // 启动后台执行线程（从断点继续）
        this.activeThread = new Thread(() -> {
            try {
                executeSteps(activePlan, sharedCtx, traceId);
            } catch (PlanCancelledException e) {
                handleImmediateCancel(traceId, e);
            } catch (Exception e) {
                handleFailure(traceId, e);
            } finally {
                cancelStepTimeout();
                cancelPlanTimeout();
            }
        }, "master-agent-" + traceId);
        activeThread.start();

        return activeSink.asFlux()
                .doOnCancel(this::cancelImmediately);
    }

    /**
     * 查询 Agent 当前生命周期状态。
     */
    public AgentState getState() {
        return state.get();
    }

    /**
     * 获取当前执行计划（仅在 RUNNING / CANCELLING 状态时有值）。
     */
    public ExecutionPlan getActivePlan() {
        return activePlan;
    }

    // ═══════════════════════════════════════════════════════════
    // 核心执行循环
    // ═══════════════════════════════════════════════════════════

    /**
     * 遍历 Plan 的 Step 列表，逐步执行。
     *
     * <p>每步执行前后保存 Checkpoint，每步后触发 Reflection。
     * 在以下时机检查取消状态：
     * <ul>
     *   <li>每个 Step 开始前（立即取消 + 优雅取消）</li>
     *   <li>每个 Step 完成后（优雅取消在此处生效）</li>
     *   <li>Reflection 前后（立即取消）</li>
     * </ul>
     *
     * <p>超时保护层级：
     * <ul>
     *   <li>Step 级：每个 Step 执行有独立超时（{@link PlanTimeoutConfig#stepTimeout}）</li>
     *   <li>Reflection 级：Reflection LLM 调用有独立超时（{@link PlanTimeoutConfig#reflectionTimeout}）</li>
     *   <li>Replan 级：Replan LLM 调用有独立超时（{@link PlanTimeoutConfig#replanTimeout}）</li>
     *   <li>Plan 级：全局超时由 {@link #schedulePlanTimeout} 控制</li>
     * </ul>
     */
    private void executeSteps(ExecutionPlan plan, QueryLoopContext sharedCtx, String traceId) {
        plan.setStatus(PlanStatus.EXECUTING);
        checkpointManager.savePlanSnapshot(plan, traceId);

        while (plan.hasNextStep()) {
            // ── 检查点 1: Step 开始前 ──
            checkImmediateCancel();

            int idx = plan.getCurrentStepIndex() + 1;
            PlanStep step = plan.getSteps().get(idx);

            // 跳过已标记 SKIPPED 或 COMPLETED 的 Step
            if (step.getStatus() == StepStatus.SKIPPED || step.getStatus() == StepStatus.COMPLETED) {
                plan.setCurrentStepIndex(idx);
                if (step.getStatus() == StepStatus.SKIPPED) {
                    log.info("[MasterAgent][{}] Step {} 已标记 SKIPPED，跳过", traceId, idx);
                }
                continue;
            }

            plan.setCurrentStepIndex(idx);

            // 依赖检查
            if (!dependenciesSatisfied(step, plan)) {
                step.setStatus(StepStatus.SKIPPED);
                log.info("[MasterAgent][{}] Step {} 跳过：依赖步骤失败或未完成", traceId, idx);
                activeSink.tryEmitNext(PlanEvent.stepSkipped(idx, "依赖步骤失败或未完成"));
                checkpointManager.saveStepCheckpoint(plan.getPlanId(), step,
                        StepCheckpointType.AFTER_FAILURE, sharedCtx.getUserId(), traceId);
                continue;
            }

            // ★ Checkpoint：Step 执行前
            checkpointManager.saveStepCheckpoint(plan.getPlanId(), step,
                    StepCheckpointType.BEFORE_EXECUTION, sharedCtx.getUserId(), traceId);

            // ★ 执行单步（带超时保护）
            executeStepWithTimeout(step, plan, sharedCtx, traceId);

            // ★ Checkpoint：Step 执行后
            StepCheckpointType ckType = step.getStatus() == StepStatus.COMPLETED
                    ? StepCheckpointType.AFTER_COMPLETION
                    : StepCheckpointType.AFTER_FAILURE;
            checkpointManager.saveStepCheckpoint(plan.getPlanId(), step,
                    ckType, sharedCtx.getUserId(), traceId);
            checkpointManager.savePlanSnapshot(plan, traceId);

            // ── 检查点 2: Step 完成后（优雅取消在此生效） ──
            if (checkGracefulCancel(traceId)) return;

            // ── Reflection（带超时保护） ──
            if (plan.hasNextStep()) {
                checkImmediateCancel();

                ReflectionResult reflection = reflectWithTimeout(plan, sharedCtx, traceId);
                if (reflection == null) return; // 超时已触发取消

                if (reflection.getAction() == ReflectionAction.REPLAN && plan.canReplan()) {
                    boolean replanOk = handleReplanWithTimeout(plan, sharedCtx, idx, reflection, traceId);
                    if (!replanOk) return; // 超时已触发取消

                } else if (reflection.getAction() == ReflectionAction.ABORT) {
                    log.warn("[MasterAgent][{}] Reflection 中止执行 reason={}",
                            traceId, reflection.getReason());
                    plan.setStatus(PlanStatus.FAILED);
                    checkpointManager.savePlanSnapshot(plan, traceId);
                    activeSink.tryEmitNext(PlanEvent.error("执行中止: " + reflection.getReason()));
                    activeSink.tryEmitComplete();
                    transitionTo(AgentState.FAILED);
                    return;
                }
                // CONTINUE → 继续下一步
            }

            // ── 检查点 3: Reflection 后（优雅取消） ──
            if (checkGracefulCancel(traceId)) return;
        }

        // 全部完成
        plan.setStatus(PlanStatus.COMPLETED);
        checkpointManager.savePlanSnapshot(plan, traceId);
        checkpointManager.markPlanCompleted(plan.getPlanId());
        String summary = buildSummary(plan);
        activeSink.tryEmitNext(PlanEvent.complete(summary));
        activeSink.tryEmitComplete();
        transitionTo(AgentState.COMPLETED);
        log.info("[MasterAgent][{}] ══════ 执行完成 {} steps ══════", traceId, plan.getSteps().size());
    }

    // ═══════════════════════════════════════════════════════════
    // 单步执行
    // ═══════════════════════════════════════════════════════════

    private void executeStep(PlanStep step, ExecutionPlan plan,
                             QueryLoopContext sharedCtx, String traceId) {
        // 立即取消检查（在 step 内部也检查）
        activeCancelToken.throwIfCancelled();

        step.setStatus(StepStatus.RUNNING);
        step.setStartedAt(System.currentTimeMillis());
        activeSink.tryEmitNext(PlanEvent.stepStart(step.getSeq(), step));

        String enrichedQuery = enrichQuery(step, plan);
        try {
            String stepResult = executeSingleStep(enrichedQuery, step.getIntent(), sharedCtx, traceId);
            step.setResult(stepResult);
            step.setStatus(StepStatus.COMPLETED);
            step.setCompletedAt(System.currentTimeMillis());
            activeSink.tryEmitNext(PlanEvent.stepComplete(step.getSeq(), step));
            log.info("[MasterAgent][{}] Step {} 完成 intent={} resultLen={}",
                    traceId, step.getSeq(), step.getIntent(),
                    stepResult != null ? stepResult.length() : 0);

            // ★ StateBus: 广播步骤完成后的状态变更
            publishStateChange(sharedCtx, traceId, "MasterAgent.step" + step.getSeq());

        } catch (PlanCancelledException e) {
            throw e; // 重新抛出，让 executeSteps 的 catch 处理
        } catch (Exception e) {
            log.error("[MasterAgent][{}] Step {} 执行失败 err={}", traceId, step.getSeq(), e.toString());
            step.setStatus(StepStatus.FAILED);
            step.setCompletedAt(System.currentTimeMillis());
            activeSink.tryEmitNext(PlanEvent.stepFailed(step.getSeq(), e.getMessage()));
        }
    }

    /**
     * 执行单步 —— Plan-and-Execute 与 Query Loop 的桥接点。
     */
    private String executeSingleStep(String enrichedQuery, IntentType intent,
                                      QueryLoopContext sharedCtx, String traceId) {
        activeCancelToken.throwIfCancelled();

        sharedCtx.setOriginalInput(enrichedQuery);
        sharedCtx.setCleanedInput(enrichedQuery.strip().replaceAll("\\s{2,}", " "));
        sharedCtx.getMetadata().put("planPreClassifiedIntent", intent.name());
        sharedCtx.getMetadata().put("skipLLMClassification", true);

        // Path A: TOOL_CALL
        if (intent == IntentType.TOOL_CALL) {
            activeCancelToken.throwIfCancelled();
            String toolResult = executeAsBusinessTool(enrichedQuery, sharedCtx, traceId);
            if (toolResult != null) return toolResult;
        }

        // Path B: 完整 Query Loop Pipeline
        activeCancelToken.throwIfCancelled();
        return queryLoopService.executeSyncWithContext(sharedCtx, traceId);
    }

    // ═══════════════════════════════════════════════════════════
    // 超时包装器
    // ═══════════════════════════════════════════════════════════

    /**
     * 带超时保护的单步执行。
     *
     * <p>机制：在调用 {@link #executeStep} 之前启动一个 watchdog 定时器，
     * 如果 Step 在 {@link PlanTimeoutConfig#stepTimeout} 内未完成，
     * watchdog 触发 {@link #cancelImmediately()} 中断执行。
     *
     * <p>正常完成时取消 watchdog，避免误触发。</p>
     */
    private void executeStepWithTimeout(PlanStep step, ExecutionPlan plan,
                                         QueryLoopContext sharedCtx, String traceId) {
        Duration timeout = timeoutConfig.stepTimeout();
        scheduleStepTimeout(timeout, step.getSeq(), traceId);
        try {
            executeStep(step, plan, sharedCtx, traceId);
        } finally {
            cancelStepTimeout();
        }
    }

    /**
     * 带超时保护的 Reflection LLM 调用。
     *
     * @return ReflectionResult，如果超时触发取消则返回 null
     */
    private ReflectionResult reflectWithTimeout(ExecutionPlan plan, QueryLoopContext sharedCtx,
                                                 String traceId) {
        Duration timeout = timeoutConfig.reflectionTimeout();
        ScheduledFuture<?> future = timeoutScheduler.schedule(() -> {
            log.warn("[MasterAgent][{}] Reflection LLM 调用超时 {}s，触发自动取消",
                    traceId, timeout.toSeconds());
            onTimeout("Reflection 超时 " + timeout.toSeconds() + "s", traceId);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        try {
            return reflector.reflect(plan, sharedCtx, traceId);
        } catch (Exception e) {
            log.error("[MasterAgent][{}] Reflection 调用异常 err={}", traceId, e.toString());
            return ReflectionResult.continue_("Reflection 异常，默认继续");
        } finally {
            future.cancel(false);
        }
    }

    /**
     * 带超时保护的 Replan LLM 调用。
     *
     * @return true 表示 replan 成功，false 表示超时已触发取消
     */
    private boolean handleReplanWithTimeout(ExecutionPlan plan, QueryLoopContext sharedCtx,
                                             int idx, ReflectionResult reflection, String traceId) {
        Duration timeout = timeoutConfig.replanTimeout();
        ScheduledFuture<?> future = timeoutScheduler.schedule(() -> {
            log.warn("[MasterAgent][{}] Replan LLM 调用超时 {}s，触发自动取消",
                    traceId, timeout.toSeconds());
            onTimeout("Replan 超时 " + timeout.toSeconds() + "s", traceId);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        try {
            handleReplan(plan, sharedCtx, idx, reflection, traceId);
            return true;
        } catch (PlanCancelledException e) {
            throw e; // 超时触发的取消，重新抛出
        } catch (Exception e) {
            log.error("[MasterAgent][{}] Replan 调用异常，降级为 CONTINUE err={}",
                    traceId, e.toString());
            return true; // 异常降级：继续下一步而非中断
        } finally {
            future.cancel(false);
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 超时定时器管理
    // ═══════════════════════════════════════════════════════════

    /**
     * 启动 Step 级超时 watchdog。
     * 超时后自动触发 {@link #cancelImmediately()}，由现有的取消链路统一处理。
     */
    private void scheduleStepTimeout(Duration timeout, int stepSeq, String traceId) {
        cancelStepTimeout(); // 先取消上一轮的定时器
        stepTimeoutFuture = timeoutScheduler.schedule(() -> {
            log.warn("[MasterAgent][{}] Step {} 执行超时 {}s，触发自动取消",
                    traceId, stepSeq, timeout.toSeconds());
            onTimeout("Step " + stepSeq + " 超时 " + timeout.toSeconds() + "s", traceId);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 取消 Step 级超时 watchdog（Step 正常完成时调用）。
     */
    private void cancelStepTimeout() {
        ScheduledFuture<?> f = stepTimeoutFuture;
        if (f != null && !f.isDone()) {
            f.cancel(false);
        }
    }

    /**
     * 启动 Plan 级超时。
     * 超时后自动触发立即停止，Checkpoint 保存由 {@link #onTimeout} 处理。
     */
    private void schedulePlanTimeout(Duration timeout, String traceId) {
        cancelPlanTimeout();
        planTimeoutFuture = timeoutScheduler.schedule(() -> {
            log.warn("[MasterAgent][{}] Plan 执行超时 {}s，触发自动取消",
                    traceId, timeout.toSeconds());
            onTimeout("Plan 超时 " + timeout.toSeconds() + "s", traceId);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 取消 Plan 级超时（正常完成或异常终止时调用）。
     */
    private void cancelPlanTimeout() {
        ScheduledFuture<?> f = planTimeoutFuture;
        if (f != null && !f.isDone()) {
            f.cancel(false);
        }
    }

    /**
     * 超时统一处理：保存 Checkpoint → 触发立即停止。
     *
     * <p>与外部取消的区别：超时消息更明确，帮助前端区分"用户取消"和"系统超时"。</p>
     */
    private void onTimeout(String reason, String traceId) {
        // 先保存当前状态，再触发取消（cancelImmediately 也会保存，这里双重保险）
        if (activePlan != null) {
            activePlan.setStatus(PlanStatus.FAILED);
            checkpointManager.savePlanSnapshot(activePlan, traceId);
        }
        // 走现有的立即取消链路：cancelToken + thread.interrupt + sink error
        cancelImmediately();
    }

    /**
     * 设置超时配置（可选，不调用则使用默认值）。
     */
    public void setTimeoutConfig(PlanTimeoutConfig config) {
        this.timeoutConfig = config != null ? config : PlanTimeoutConfig.defaults();
        log.info("[MasterAgent] 超时配置已更新 step={} plan={} reflection={} replan={} infra={}",
                config.stepTimeout(), config.planTimeout(),
                config.reflectionTimeout(), config.replanTimeout(),
                config.infraTimeout());
    }

    // ═══════════════════════════════════════════════════════════
    // 取消 / 失败处理
    // ═══════════════════════════════════════════════════════════

    /** 立即取消：CancellationToken 触发后的统一处理 */
    private void handleImmediateCancel(String traceId, PlanCancelledException e) {
        log.info("[MasterAgent][{}] Plan 被立即取消 planId={}", traceId, e.getPlanId());
        if (activePlan != null) {
            activePlan.setStatus(PlanStatus.FAILED);
            checkpointManager.savePlanSnapshot(activePlan, traceId);
        }
        Sinks.Many<PlanEvent> sink = activeSink;
        if (sink != null) {
            sink.tryEmitNext(PlanEvent.error("执行被取消"));
            sink.tryEmitComplete();
        }
        transitionTo(AgentState.CANCELLED);
    }

    /** 执行异常的统一处理 */
    private void handleFailure(String traceId, Exception e) {
        log.error("[MasterAgent][{}] 执行异常 err={}", traceId, e.toString());
        if (activePlan != null) {
            activePlan.setStatus(PlanStatus.FAILED);
            checkpointManager.savePlanSnapshot(activePlan, traceId);
        }
        Sinks.Many<PlanEvent> sink = activeSink;
        if (sink != null) {
            sink.tryEmitNext(PlanEvent.error(e.getMessage()));
            sink.tryEmitComplete();
        }
        transitionTo(AgentState.FAILED);
    }

    /**
     * 检查立即取消：如果 CancellationToken 已触发，直接抛异常中断。
     */
    private void checkImmediateCancel() {
        if (activeCancelToken != null) {
            activeCancelToken.throwIfCancelled();
        }
    }

    /**
     * 检查优雅取消：如果状态为 CANCELLING（即当前 Step 已执行完毕），
     * 则保存 Checkpoint、发送事件、关闭 Sink、状态迁移。
     *
     * @return true 表示已触发优雅停止，调用方应立即 return
     */
    private boolean checkGracefulCancel(String traceId) {
        if (state.get() != AgentState.CANCELLING) {
            return false;
        }

        log.info("[MasterAgent][{}] 优雅停止生效 planId={}，当前 Step 已完成，Checkpoint 已保存",
                traceId, activePlan != null ? activePlan.getPlanId() : "null");

        // Checkpoint 已在上层保存，这里只需关闭流
        Sinks.Many<PlanEvent> sink = activeSink;
        if (sink != null) {
            sink.tryEmitNext(PlanEvent.error("执行已被优雅停止，可通过 resume 接口恢复"));
            sink.tryEmitComplete();
        }
        transitionTo(AgentState.CANCELLED);
        return true;
    }

    // ═══════════════════════════════════════════════════════════
    // Reflection / Replan
    // ═══════════════════════════════════════════════════════════

    private void handleReplan(ExecutionPlan plan, QueryLoopContext sharedCtx,
                               int idx, ReflectionResult reflection, String traceId) {
        log.info("[MasterAgent][{}] Reflection 触发重规划 reason={}", traceId, reflection.getReason());
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
            ns.setStepId(UUID.randomUUID().toString().substring(0, 8));
            ns.setSeq(nextSeq++);
            plan.getSteps().add(ns);
        }

        // ★ replan 后 checkpoint
        checkpointManager.savePlanSnapshot(plan, traceId);
        activeSink.tryEmitNext(PlanEvent.replan(plan, reflection.getReason()));
    }

    // ═══════════════════════════════════════════════════════════
    // 业务工具直接调用
    // ═══════════════════════════════════════════════════════════

    /**
     * 尝试关键词匹配执行业务工具，跳过 LLM 分类。
     * 返回 null 表示未匹配，调用方回退到完整 Pipeline。
     */
    private String executeAsBusinessTool(String query, QueryLoopContext sharedCtx, String traceId) {
        if (query.contains("ORD") || query.contains("订单")) {
            String orderId = extractParam(query, ORDER_ID_PATTERN, "ORD001");
            ToolResult result = planner.executeBusinessTool("orderQuery",
                    Map.of("orderId", orderId));
            return formatToolResult(result, sharedCtx, "orderQuery");
        }

        if (query.contains("TN") || query.contains("物流") || query.contains("单号")) {
            String trackingNo = extractParam(query, TRACKING_PATTERN, "TN001");
            ToolResult result = planner.executeBusinessTool("logisticsTrace",
                    Map.of("trackingNumber", trackingNo));
            return formatToolResult(result, sharedCtx, "logisticsTrace");
        }

        if (query.contains("工单") || query.contains("创建")) {
            String patientName = extractName(query);
            ToolResult result = planner.executeBusinessTool("workOrderCreate",
                    Map.of("patientName", patientName, "serviceType", "护理服务"));
            return formatToolResult(result, sharedCtx, "workOrderCreate");
        }

        return null;
    }

    private String extractParam(String text, Pattern pattern, String defaultVal) {
        java.util.regex.Matcher m = pattern.matcher(text);
        return m.find() ? m.group() : defaultVal;
    }

    private String extractName(String text) {
        java.util.regex.Matcher m = NAME_PATTERN.matcher(text);
        return m.find() ? m.group().replaceAll("(?:老人|奶奶|爷爷|先生|女士)", "") : "未指定";
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

    // ═══════════════════════════════════════════════════════════
    // 辅助方法
    // ═══════════════════════════════════════════════════════════

    private boolean dependenciesSatisfied(PlanStep step, ExecutionPlan plan) {
        if (step.getDependsOn() == null || step.getDependsOn().isEmpty()) return true;
        for (int depSeq : step.getDependsOn()) {
            PlanStep dep = plan.getSteps().stream()
                    .filter(s -> s.getSeq() == depSeq).findFirst().orElse(null);
            if (dep == null || dep.getStatus() != StepStatus.COMPLETED) return false;
        }
        return true;
    }

    private String enrichQuery(PlanStep step, ExecutionPlan plan) {
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

    /**
     * 重放已完成的 Step SSE 事件，用于恢复时前端 UI 状态重建。
     */
    private void replayStepEvent(PlanStep s, Sinks.Many<PlanEvent> sink, String traceId) {
        switch (s.getStatus()) {
            case COMPLETED:
                log.debug("[MasterAgent][{}] 重放 Step {} COMPLETED", traceId, s.getSeq());
                sink.tryEmitNext(PlanEvent.stepComplete(s.getSeq(), s));
                break;
            case FAILED:
                log.debug("[MasterAgent][{}] 重放 Step {} FAILED", traceId, s.getSeq());
                sink.tryEmitNext(PlanEvent.stepFailed(s.getSeq(), "前序执行失败"));
                break;
            case SKIPPED:
                log.debug("[MasterAgent][{}] 重放 Step {} SKIPPED", traceId, s.getSeq());
                sink.tryEmitNext(PlanEvent.stepSkipped(s.getSeq(), "重规划时跳过"));
                break;
            default:
                break;
        }
    }

    private void transitionTo(AgentState target) {
        AgentState prev = state.getAndSet(target);
        log.info("[MasterAgent] 状态迁移: {} → {}", prev, target);
    }

    /**
     * 通过 StateBus 广播状态变更，其他 Agent 可订阅并感知。
     */
    private void publishStateChange(QueryLoopContext sharedCtx, String traceId, String source) {
        try {
            SessionState current = sharedCtx.getSessionState();
            if (current != null) {
                SessionState previous = stateBus.getSnapshot(sharedCtx.getUserId());
                stateBus.publish(sharedCtx.getUserId(), previous, current, source);
                log.debug("[MasterAgent][{}] StateBus 已发布 userId={} src={}",
                        traceId, sharedCtx.getUserId(), source);
            }
        } catch (Exception e) {
            log.warn("[MasterAgent][{}] StateBus 发布失败 err={}", traceId, e.toString());
        }
    }
}
