package com.queryloop;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionState {

    private String userId;
    private IntentType lastIntent = IntentType.GENERAL_CHAT;
    private double lastConfidence = 0.0;
    private IntentType activeContext = IntentType.GENERAL_CHAT;
    private int totalMessages = 0;
    private List<String> activeDocSnippets = new ArrayList<>();
    private String lastToolName = "";
    private boolean firstSession = true;
    private List<String> recentIntents = new ArrayList<>();
    private LocalDateTime createdAt = LocalDateTime.now();
    private LocalDateTime updatedAt = LocalDateTime.now();

    // ═══════════════════════════════════════════════
    // Phase 1: 意图反馈追踪
    // ═══════════════════════════════════════════════

    /**
     * 按意图类型分组的反馈统计
     * Key = IntentType.name(), Value = IntentFeedbackStats
     * 使用 LinkedHashMap 保证遍历顺序
     */
    private Map<String, IntentFeedbackStats> feedbackByIntent = new LinkedHashMap<>();

    /**
     * 当前轮次的分类结果（用于后续反馈关联，不持久化到 Redis）
     */
    @JsonIgnore
    private transient IntentFeedbackRecord pendingFeedback;

    // ═══════════════════════════════════════════════
    // Phase 2: 工具调用结果缓存
    // ═══════════════════════════════════════════════

    /**
     * 最近一次工具调用的结构化结果
     * 供后续追问时复用（如分页、列表延续等）
     */
    private ToolResult lastToolResult;

    /**
     * 按工具名缓存的最近结果 Map
     */
    private Map<String, ToolResult> toolResultCache = new LinkedHashMap<>();

    // ═══════════════════════════════════════════════
    // 派生判断 (不变)
    // ═══════════════════════════════════════════════

    public boolean isToolCallContinuation() {
        return activeContext == IntentType.TOOL_CALL;
    }

    public boolean isRagContinuation() {
        return activeContext == IntentType.RAG_SEARCH;
    }

    public boolean isSameIntent(IntentType currentIntent) {
        return lastIntent == currentIntent;
    }

    public boolean isFollowUp(IntentType classifiedIntent) {
        if (classifiedIntent == IntentType.MEMORY_QUERY) return true;
        return isSameIntent(classifiedIntent) && classifiedIntent != IntentType.GENERAL_CHAT;
    }

    // ═══════════════════════════════════════════════
    // 记录方法 (增强)
    // ═══════════════════════════════════════════════

    public void recordIntent(IntentType intent, double confidence) {
        this.lastIntent = intent;
        this.lastConfidence = confidence;
        this.recentIntents.add(intent.name());
        if (this.recentIntents.size() > 3) {
            this.recentIntents.remove(0);
        }
        this.updatedAt = LocalDateTime.now();
    }

    public void activateContext(IntentType context) {
        this.activeContext = context;
    }

    public void recordToolCall(String toolName) {
        this.lastToolName = toolName;
    }

    public void incrementMessages() {
        this.totalMessages += 2;
        this.firstSession = false;
    }

    // ═══════════════════════════════════════════════
    // Phase 1: 反馈方法
    // ═══════════════════════════════════════════════

    /** 记录一条采纳反馈 */
    public void recordAdoption(IntentFeedbackRecord record) {
        getOrCreateStats(record.getClassifiedIntent()).recordAdopted(record);
    }

    /** 记录一条纠正反馈 */
    public void recordCorrection(IntentFeedbackRecord record) {
        getOrCreateStats(record.getClassifiedIntent()).recordCorrected(record);
    }

    /** 记录一条忽略反馈 */
    public void recordIgnore(IntentFeedbackRecord record) {
        getOrCreateStats(record.getClassifiedIntent()).recordIgnored(record);
    }

    private IntentFeedbackStats getOrCreateStats(IntentType intent) {
        return feedbackByIntent.computeIfAbsent(intent.name(), k -> new IntentFeedbackStats());
    }

    /** 获取某意图类型的当前置信度惩罚系数 */
    public double getConfidencePenalty(IntentType intent) {
        IntentFeedbackStats stats = feedbackByIntent.get(intent.name());
        return stats != null ? stats.getConfidencePenalty() : 1.0;
    }

    /** 获取某意图类型的采纳率 */
    public double getAdoptionRate(IntentType intent) {
        IntentFeedbackStats stats = feedbackByIntent.get(intent.name());
        return stats != null ? stats.getAdoptionRate() : 1.0;
    }

    // ═══════════════════════════════════════════════
    // Phase 2: 工具结果方法
    // ═══════════════════════════════════════════════

    public void cacheToolResult(String toolName, ToolResult result) {
        this.lastToolResult = result;
        this.toolResultCache.put(toolName, result);
        this.lastToolName = toolName;
    }

    public ToolResult getLastToolResult() {
        return lastToolResult;
    }

    public ToolResult getToolResult(String toolName) {
        return toolResultCache.get(toolName);
    }
}
