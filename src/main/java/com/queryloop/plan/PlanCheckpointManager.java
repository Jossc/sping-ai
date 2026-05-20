package com.queryloop.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.queryloop.IntentType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Plan 执行检查点管理器。
 *
 * <h3>两级 checkpoint 设计</h3>
 * <table>
 *   <tr><th>层级</th><th>存储</th><th>Key 格式</th><th>内容</th></tr>
 *   <tr><td>Plan 级</td><td>Redis String (JSON)</td><td>{@code plan:checkpoint:{planId}}</td><td>ExecutionPlan 完整快照</td></tr>
 *   <tr><td>Step 级</td><td>Redis Hash</td><td>{@code plan:checkpoint:{planId}:step:{seq}}</td><td>单步输入/输出/状态 + Context 附加信息</td></tr>
 * </table>
 *
 * <p>两级均使用 24 小时 TTL，过期自动清理。正常完成时通过 {@link #markPlanCompleted} 主动清理。</p>
 */
@Slf4j
@Component
public class PlanCheckpointManager {

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    private static final Duration CHECKPOINT_TTL = Duration.ofHours(24);

    // ── Key 命名规范 ──
    static final String PLAN_KEY_PREFIX = "plan:checkpoint:";
    static final String STEP_KEY_INFIX = ":step:";
    static final String STEP_INDEX_SUFFIX = ":step_keys";

    /** 单条 step result 在 Hash 中存储的上限（10KB），超出部分截断 */
    private static final int MAX_RESULT_LENGTH = 10_240;

    public PlanCheckpointManager(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    // ═══════════════════════════════════════════════════════════
    // Phase 1: Plan 级 checkpoint — Redis String (JSON)
    // ═══════════════════════════════════════════════════════════

    /**
     * 保存 ExecutionPlan 完整快照。
     *
     * <p>包含所有 Plan 元信息 + 全部 Step 的当前状态。
     * 用于中断恢复：可从该快照完整重建 ExecutionPlan 对象。</p>
     *
     * @param plan    当前执行计划
     * @param traceId 调用链追踪 ID
     */
    public void savePlanSnapshot(ExecutionPlan plan, String traceId) {
        if (plan == null || plan.getPlanId() == null) {
            log.warn("[CKPT][{}] savePlanSnapshot 跳过：plan 或 planId 为 null", traceId);
            return;
        }

        try {
            String key = planKey(plan.getPlanId());
            Map<String, Object> snapshot = buildPlanSnapshot(plan, traceId);
            String json = mapper.writeValueAsString(snapshot);

            redis.opsForValue().set(key, json, CHECKPOINT_TTL);

            log.debug("[CKPT][{}] Plan 快照已保存 planId={} stepIndex={}/{} status={}",
                    traceId, plan.getPlanId(), plan.getCurrentStepIndex(),
                    plan.getSteps().size(), plan.getStatus());

        } catch (JsonProcessingException e) {
            log.error("[CKPT][{}] Plan 快照序列化失败 planId={} err={}", traceId, plan.getPlanId(), e.toString());
        } catch (DataAccessException e) {
            log.error("[CKPT][{}] Plan 快照 Redis 写入失败 planId={} err={}", traceId, plan.getPlanId(), e.toString());
        } catch (Exception e) {
            log.error("[CKPT][{}] Plan 快照未知异常 planId={} err={}", traceId, plan.getPlanId(), e.toString());
        }
    }

    /**
     * 尝试从 Plan 级 checkpoint 恢复 ExecutionPlan。
     *
     * <p>恢复逻辑：
     * <ol>
     *   <li>读 Redis String → 反序列化为 Map</li>
     *   <li>校验 Plan 状态：COMPLETED 则返回 notFound</li>
     *   <li>重建 ExecutionPlan + 所有 PlanStep</li>
     *   <li>RUNNING 状态 step 重置为 PENDING（崩溃安全）</li>
     *   <li>定位断点位置（第一个 PENDING step）</li>
     * </ol>
     *
     * @param planId 计划 ID
     * @return 恢复结果；不可恢复时返回 {@link PlanRecoveryResult#notFound()}
     */
    @SuppressWarnings("unchecked")
    public PlanRecoveryResult tryRecover(String planId) {
        if (planId == null || planId.isBlank()) {
            return PlanRecoveryResult.notFound();
        }

        try {
            String key = planKey(planId);
            String json = redis.opsForValue().get(key);
            if (json == null || json.isEmpty()) {
                log.debug("[CKPT] Plan checkpoint 不存在 planId={}", planId);
                return PlanRecoveryResult.notFound();
            }

            Map<String, Object> snapshot = mapper.readValue(json, Map.class);

            // 校验基本字段完整性
            if (!snapshot.containsKey("planId") || !snapshot.containsKey("steps")) {
                log.warn("[CKPT] Plan checkpoint 数据残缺 planId={}", planId);
                return PlanRecoveryResult.notFound();
            }

            // 已完成则不需要恢复
            String status = (String) snapshot.get("status");
            if (PlanStatus.COMPLETED.name().equals(status)) {
                log.debug("[CKPT] Plan 已完成，无需恢复 planId={}", planId);
                return PlanRecoveryResult.notFound();
            }

            ExecutionPlan plan = restorePlanFromSnapshot(snapshot);
            if (plan == null) {
                return PlanRecoveryResult.notFound();
            }

            // 定位断点
            int resumeFrom = locateResumePoint(plan, snapshot);
            plan.setCurrentStepIndex(resumeFrom - 1);

            log.info("[CKPT] Plan 恢复成功 planId={} resumeFromStep={} totalSteps={}",
                    planId, resumeFrom, plan.getSteps().size());

            return PlanRecoveryResult.recoverable(plan, resumeFrom);

        } catch (JsonProcessingException e) {
            log.error("[CKPT] Plan checkpoint JSON 反序列化失败 planId={} err={}", planId, e.toString());
            return PlanRecoveryResult.notFound();
        } catch (DataAccessException e) {
            log.error("[CKPT] Plan checkpoint Redis 读取失败 planId={} err={}", planId, e.toString());
            return PlanRecoveryResult.notFound();
        } catch (Exception e) {
            log.error("[CKPT] Plan 恢复未知异常 planId={} err={}", planId, e.toString());
            return PlanRecoveryResult.notFound();
        }
    }

    // ═══════════════════════════════════════════════════════════
    // Phase 2: Step 级 checkpoint — Redis Hash
    // ═══════════════════════════════════════════════════════════

    /**
     * 保存单步 checkpoint（含 Context 附加信息）。
     *
     * <p>Hash 字段分为三类：</p>
     * <ul>
     *   <li><b>Step 基础信息</b>：seq, stepId, intent, subQuery, reasoning, dependsOn</li>
     *   <li><b>Step 执行状态</b>：status, result, toolResultJson, startedAt, completedAt, elapsedMs</li>
     *   <li><b>Context 附加信息</b>：planId, traceId, checkpointType, savedAt</li>
     * </ul>
     *
     * @param planId 所属计划 ID
     * @param step   当前步骤
     * @param type   检查点类型（执行前/完成后/失败后）
     */
    public void saveStepCheckpoint(String planId, PlanStep step, StepCheckpointType type) {
        saveStepCheckpoint(planId, step, type, null, null);
    }

    /**
     * 保存单步 checkpoint，附带调用链上下文。
     *
     * @param planId  所属计划 ID
     * @param step    当前步骤
     * @param type    检查点类型
     * @param userId  用户 ID（Context 附加信息，可为 null）
     * @param traceId 追踪 ID（Context 附加信息，可为 null）
     */
    public void saveStepCheckpoint(String planId, PlanStep step, StepCheckpointType type,
                                    String userId, String traceId) {
        if (planId == null || step == null) {
            log.warn("[CKPT] saveStepCheckpoint 跳过：planId 或 step 为 null");
            return;
        }

        try {
            String key = stepKey(planId, step.getSeq());
            Map<String, String> fields = buildStepFields(planId, step, type, userId, traceId);

            // 使用 pipeline 保证 putAll + expire 原子性
            redis.executePipelined((RedisCallback<?>) (connection) -> {
                byte[] rawKey = redis.getStringSerializer().serialize(key);
                if (rawKey == null) return null;

                Map<byte[], byte[]> rawFields = new LinkedHashMap<>();
                for (Map.Entry<String, String> e : fields.entrySet()) {
                    byte[] fk = redis.getStringSerializer().serialize(e.getKey());
                    byte[] fv = redis.getStringSerializer().serialize(e.getValue());
                    if (fk != null && fv != null) {
                        rawFields.put(fk, fv);
                    }
                }
                connection.hashCommands().hMSet(rawKey, rawFields);
                connection.keyCommands().expire(rawKey, CHECKPOINT_TTL.getSeconds());
                return null;
            });

            // 将 step key 注册到索引 Set，便于统一清理
            redis.opsForSet().add(stepIndexKey(planId), String.valueOf(step.getSeq()));
            redis.expire(stepIndexKey(planId), CHECKPOINT_TTL);

            log.debug("[CKPT][{}] Step 快照已保存 planId={} seq={} intent={} type={}",
                    traceId, planId, step.getSeq(), step.getIntent(), type);

        } catch (DataAccessException e) {
            log.error("[CKPT] Step 快照 Redis 写入失败 planId={} seq={} err={}",
                    planId, step.getSeq(), e.toString());
        } catch (Exception e) {
            log.error("[CKPT] Step 快照未知异常 planId={} seq={} err={}",
                    planId, step.getSeq(), e.toString());
        }
    }

    /**
     * 读取指定 step 的 checkpoint。
     *
     * @param planId 计划 ID
     * @param seq    步骤序号
     * @return step 字段 Map；不存在或读取失败返回空 Map
     */
    public Map<Object, Object> getStepCheckpoint(String planId, int seq) {
        if (planId == null) return Collections.emptyMap();

        try {
            String key = stepKey(planId, seq);
            Map<Object, Object> entries = redis.opsForHash().entries(key);
            return entries != null ? entries : Collections.emptyMap();
        } catch (DataAccessException e) {
            log.error("[CKPT] Step checkpoint 读取失败 planId={} seq={} err={}", planId, seq, e.toString());
            return Collections.emptyMap();
        }
    }

    /**
     * 获取指定 Plan 下所有已保存的 step 序号。
     */
    public Set<Integer> getSavedStepSeqs(String planId) {
        if (planId == null) return Collections.emptySet();

        try {
            Set<String> members = redis.opsForSet().members(stepIndexKey(planId));
            if (members == null || members.isEmpty()) return Collections.emptySet();
            return members.stream()
                    .map(Integer::parseInt)
                    .collect(Collectors.toSet());
        } catch (DataAccessException | NumberFormatException e) {
            log.error("[CKPT] Step 索引读取失败 planId={} err={}", planId, e.toString());
            return Collections.emptySet();
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 生命周期管理
    // ═══════════════════════════════════════════════════════════

    /**
     * 标记 Plan 完成并清理所有关联的 checkpoint 数据。
     *
     * <p>清理范围：
     * <ul>
     *   <li>Plan 级快照 (String)</li>
     *   <li>所有 Step 级快照 (Hash) — 通过 step_keys 索引定位</li>
     *   <li>Step 索引 (Set)</li>
     * </ul>
     *
     * @param planId 计划 ID
     */
    public void markPlanCompleted(String planId) {
        if (planId == null) return;

        try {
            // 收集需要删除的所有 key
            List<String> keysToDelete = new ArrayList<>();
            keysToDelete.add(planKey(planId));

            // 从索引 Set 获取所有 step seq
            Set<String> stepSeqs = redis.opsForSet().members(stepIndexKey(planId));
            if (stepSeqs != null) {
                for (String seq : stepSeqs) {
                    keysToDelete.add(stepKey(planId, Integer.parseInt(seq)));
                }
            }
            keysToDelete.add(stepIndexKey(planId));

            if (keysToDelete.size() > 1) {
                redis.delete(keysToDelete);
                log.debug("[CKPT] Plan checkpoint 已清理 planId={} keysDeleted={}",
                        planId, keysToDelete.size());
            } else {
                // 只有 plan key 和 step_index key（可能无 step）
                redis.delete(keysToDelete);
                log.debug("[CKPT] Plan checkpoint 已清理 planId={} (无 step 记录)", planId);
            }

        } catch (DataAccessException e) {
            log.error("[CKPT] Plan checkpoint 清理失败 planId={} err={}", planId, e.toString());
        }
    }

    /**
     * 检查指定 Plan 的 checkpoint 是否存在。
     */
    public boolean exists(String planId) {
        if (planId == null) return false;
        try {
            Boolean hasKey = redis.hasKey(planKey(planId));
            return Boolean.TRUE.equals(hasKey);
        } catch (DataAccessException e) {
            log.error("[CKPT] exists 查询失败 planId={} err={}", planId, e.toString());
            return false;
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 内部：Plan 快照构建 & 恢复
    // ═══════════════════════════════════════════════════════════

    @SuppressWarnings("unchecked")
    private ExecutionPlan restorePlanFromSnapshot(Map<String, Object> snapshot) {
        try {
            ExecutionPlan plan = new ExecutionPlan()
                    .setPlanId((String) snapshot.get("planId"))
                    .setOriginalQuery((String) snapshot.get("originalQuery"))
                    .setCleanedQuery((String) snapshot.get("cleanedQuery"))
                    .setPlanRationale((String) snapshot.get("planRationale"))
                    .setPlanConfidence(toDouble(snapshot.get("planConfidence")))
                    .setReplanCount(toInt(snapshot.get("replanCount")));

            // 恢复 createdAt
            Object createdAt = snapshot.get("createdAt");
            if (createdAt instanceof String) {
                plan.setCreatedAt(java.time.LocalDateTime.parse((String) createdAt));
            }

            // 恢复步骤
            List<Map<String, Object>> stepsData = (List<Map<String, Object>>) snapshot.get("steps");
            if (stepsData == null || stepsData.isEmpty()) {
                log.warn("[CKPT] Plan 快照中无步骤数据 planId={}", snapshot.get("planId"));
                return null;
            }

            for (Map<String, Object> sd : stepsData) {
                PlanStep step = restoreStepFromData(sd);
                if (step != null) {
                    plan.getSteps().add(step);
                }
            }

            if (plan.getSteps().isEmpty()) {
                log.warn("[CKPT] Plan 快照步骤恢复为空 planId={}", snapshot.get("planId"));
                return null;
            }

            int currentStepIndex = toInt(snapshot.get("currentStepIndex"));
            plan.setCurrentStepIndex(Math.max(-1, currentStepIndex));

            return plan;

        } catch (Exception e) {
            log.error("[CKPT] Plan 快照重建失败 planId={} err={}", snapshot.get("planId"), e.toString());
            return null;
        }
    }

    private PlanStep restoreStepFromData(Map<String, Object> sd) {
        try {
            String intentStr = (String) sd.get("intent");
            IntentType intent;
            try {
                intent = IntentType.valueOf(intentStr);
            } catch (IllegalArgumentException e) {
                intent = IntentType.GENERAL_CHAT;
            }

            String statusStr = (String) sd.get("status");
            StepStatus stepStatus;
            try {
                stepStatus = StepStatus.valueOf(statusStr);
            } catch (IllegalArgumentException e) {
                stepStatus = StepStatus.PENDING;
            }

            PlanStep step = new PlanStep()
                    .setStepId((String) sd.get("stepId"))
                    .setSeq(toInt(sd.get("seq")))
                    .setIntent(intent)
                    .setSubQuery((String) sd.get("subQuery"))
                    .setReasoning((String) sd.get("reasoning"))
                    .setStatus(stepStatus);

            if (sd.get("dependsOn") instanceof List) {
                step.setDependsOn((List<Integer>) sd.get("dependsOn"));
            }
            if (sd.get("result") != null) {
                step.setResult((String) sd.get("result"));
            }
            if (sd.get("toolResultJson") != null) {
                step.setToolResultJson((String) sd.get("toolResultJson"));
            }
            if (sd.get("startedAt") != null) {
                step.setStartedAt(toLong(sd.get("startedAt")));
            }
            if (sd.get("completedAt") != null) {
                step.setCompletedAt(toLong(sd.get("completedAt")));
            }

            return step;

        } catch (Exception e) {
            log.error("[CKPT] Step 重建失败 seq={} err={}", sd.get("seq"), e.toString());
            return null;
        }
    }

    /**
     * 定位断点：找到第一个 status 为 PENDING 或 RUNNING 的 step。
     * RUNNING 状态的 step 会先被重置为 PENDING。
     */
    private int locateResumePoint(ExecutionPlan plan, Map<String, Object> snapshot) {
        int firstPending = 0;
        boolean foundPending = false;

        for (PlanStep s : plan.getSteps()) {
            if (!foundPending
                    && (s.getStatus() == StepStatus.PENDING || s.getStatus() == StepStatus.RUNNING)) {
                firstPending = s.getSeq();
                foundPending = true;
            }
        }

        // 重置 RUNNING → PENDING（崩溃安全：上次可能执行到一半）
        for (PlanStep s : plan.getSteps()) {
            if (s.getStatus() == StepStatus.RUNNING) {
                log.warn("[CKPT] Step {} 状态为 RUNNING（疑似崩溃），重置为 PENDING", s.getSeq());
                s.setStatus(StepStatus.PENDING);
                s.setResult(null);
                s.setToolResultJson(null);
                s.setStartedAt(0);
                s.setCompletedAt(0);
            }
        }

        if (foundPending) {
            return firstPending;
        }
        // 所有 step 都已完成/失败/跳过 → 从最后一步之后恢复（即触发完成）
        return toInt(snapshot.get("currentStepIndex")) + 1;
    }

    // ═══════════════════════════════════════════════════════════
    // 内部：序列化辅助
    // ═══════════════════════════════════════════════════════════

    private Map<String, Object> buildPlanSnapshot(ExecutionPlan plan, String traceId) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("planId", plan.getPlanId());
        snapshot.put("originalQuery", plan.getOriginalQuery());
        snapshot.put("cleanedQuery", plan.getCleanedQuery());
        snapshot.put("status", plan.getStatus().name());
        snapshot.put("currentStepIndex", plan.getCurrentStepIndex());
        snapshot.put("planConfidence", plan.getPlanConfidence());
        snapshot.put("planRationale", plan.getPlanRationale());
        snapshot.put("replanCount", plan.getReplanCount());
        snapshot.put("stepCount", plan.getSteps().size());
        snapshot.put("traceId", traceId);
        snapshot.put("savedAt", System.currentTimeMillis());
        if (plan.getCreatedAt() != null) {
            snapshot.put("createdAt", plan.getCreatedAt().toString());
        }

        List<Map<String, Object>> steps = new ArrayList<>();
        for (PlanStep s : plan.getSteps()) {
            steps.add(buildStepData(s));
        }
        snapshot.put("steps", steps);

        return snapshot;
    }

    private Map<String, Object> buildStepData(PlanStep s) {
        Map<String, Object> sm = new LinkedHashMap<>();
        sm.put("stepId", s.getStepId());
        sm.put("seq", s.getSeq());
        sm.put("intent", s.getIntent().name());
        sm.put("subQuery", s.getSubQuery());
        sm.put("reasoning", s.getReasoning());
        sm.put("dependsOn", s.getDependsOn() != null ? s.getDependsOn() : Collections.emptyList());
        sm.put("status", s.getStatus().name());
        sm.put("result", s.getResult());
        if (s.getToolResultJson() != null) {
            sm.put("toolResultJson", s.getToolResultJson());
        }
        sm.put("startedAt", s.getStartedAt());
        sm.put("completedAt", s.getCompletedAt());
        return sm;
    }

    private Map<String, String> buildStepFields(String planId, PlanStep step,
                                                 StepCheckpointType type,
                                                 String userId, String traceId) {
        Map<String, String> fields = new LinkedHashMap<>();

        // ── Step 基础信息 ──
        fields.put("planId", planId);
        fields.put("seq", String.valueOf(step.getSeq()));
        fields.put("stepId", step.getStepId() != null ? step.getStepId() : "");
        fields.put("intent", step.getIntent() != null ? step.getIntent().name() : IntentType.GENERAL_CHAT.name());
        fields.put("subQuery", step.getSubQuery() != null ? step.getSubQuery() : "");
        fields.put("reasoning", step.getReasoning() != null ? step.getReasoning() : "");
        fields.put("dependsOn", step.getDependsOn() != null
                ? step.getDependsOn().stream().map(String::valueOf).collect(Collectors.joining(","))
                : "");

        // ── Step 执行状态 ──
        fields.put("status", step.getStatus() != null ? step.getStatus().name() : StepStatus.PENDING.name());
        fields.put("result", truncateResult(step.getResult()));
        fields.put("toolResultJson", step.getToolResultJson() != null ? step.getToolResultJson() : "");
        fields.put("startedAt", String.valueOf(step.getStartedAt()));
        fields.put("completedAt", String.valueOf(step.getCompletedAt()));
        fields.put("elapsedMs", step.getCompletedAt() > 0 && step.getStartedAt() > 0
                ? String.valueOf(step.getCompletedAt() - step.getStartedAt())
                : "0");

        // ── Context 附加信息 ──
        fields.put("checkpointType", type.name());
        fields.put("savedAt", String.valueOf(System.currentTimeMillis()));
        if (userId != null) {
            fields.put("userId", userId);
        }
        if (traceId != null) {
            fields.put("traceId", traceId);
        }

        return fields;
    }

    /**
     * 对 step result 做安全截断，避免超长 LLM 响应撑爆 Redis Hash。
     */
    private String truncateResult(String result) {
        if (result == null) return "";
        if (result.length() <= MAX_RESULT_LENGTH) return result;
        return result.substring(0, MAX_RESULT_LENGTH)
                + "...[truncated at " + MAX_RESULT_LENGTH + " chars, original length="
                + result.length() + "]";
    }

    // ═══════════════════════════════════════════════════════════
    // 内部：Key 工具方法
    // ═══════════════════════════════════════════════════════════

    static String planKey(String planId) {
        return PLAN_KEY_PREFIX + planId;
    }

    static String stepKey(String planId, int seq) {
        return PLAN_KEY_PREFIX + planId + STEP_KEY_INFIX + seq;
    }

    static String stepIndexKey(String planId) {
        return PLAN_KEY_PREFIX + planId + STEP_INDEX_SUFFIX;
    }

    // ═══════════════════════════════════════════════════════════
    // 内部：安全类型转换
    // ═══════════════════════════════════════════════════════════

    private static double toDouble(Object value) {
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof String) {
            try { return Double.parseDouble((String) value); } catch (NumberFormatException e) { return 0.0; }
        }
        return 0.0;
    }

    private static int toInt(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) {
            try { return Integer.parseInt((String) value); } catch (NumberFormatException e) { return 0; }
        }
        return 0;
    }

    private static long toLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        if (value instanceof String) {
            try { return Long.parseLong((String) value); } catch (NumberFormatException e) { return 0L; }
        }
        return 0L;
    }

    // ═══════════════════════════════════════════════════════════
    // 内部：恢复结果
    // ═══════════════════════════════════════════════════════════

    /**
     * Plan 快照恢复结果。
     */
    public static class PlanRecoveryResult {
        private final boolean recoverable;
        private final ExecutionPlan plan;
        private final int resumeFromSeq;

        private PlanRecoveryResult(boolean recoverable, ExecutionPlan plan, int resumeFromSeq) {
            this.recoverable = recoverable;
            this.plan = plan;
            this.resumeFromSeq = resumeFromSeq;
        }

        public static PlanRecoveryResult notFound() {
            return new PlanRecoveryResult(false, null, 0);
        }

        public static PlanRecoveryResult recoverable(ExecutionPlan plan, int resumeFromSeq) {
            return new PlanRecoveryResult(true, plan, resumeFromSeq);
        }

        public boolean isRecoverable() { return recoverable; }
        public ExecutionPlan getPlan() { return plan; }
        public int getResumeFromSeq() { return resumeFromSeq; }
    }
}
