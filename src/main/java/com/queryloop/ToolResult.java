package com.queryloop;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 工具调用结果
 */
@Data
@Accessors(chain = true)
public class ToolResult {

    /** 工具名称 */
    private String toolName;

    /** 调用是否成功 */
    private boolean success = true;

    /** 错误信息（失败时填充） */
    private String errorMessage;

    /** 耗时毫秒 */
    private long elapsedMs;

    /** 是否触发了降级回复 */
    private boolean fallback = false;

    /** 结果数据（结构化） */
    private Map<String, Object> data = new LinkedHashMap<>();

    /** 人类可读的结果摘要 */
    private String summary = "";

    /** 分页信息（分页工具时填充） */
    private Pagination pagination;

    private LocalDateTime timestamp = LocalDateTime.now();

    @Data
    @Accessors(chain = true)
    public static class Pagination {
        private int currentPage = 0;
        private int pageSize = 10;
        private int totalItems = 0;
        private int totalPages = 0;
        private boolean hasMore = false;
    }

    // ── 工厂方法 ──

    public static ToolResult success(String toolName, String summary, long elapsedMs) {
        return new ToolResult()
                .setToolName(toolName)
                .setSuccess(true)
                .setSummary(summary)
                .setElapsedMs(elapsedMs);
    }

    public static ToolResult fail(String toolName, String error, long elapsedMs) {
        return new ToolResult()
                .setToolName(toolName)
                .setSuccess(false)
                .setErrorMessage(error)
                .setElapsedMs(elapsedMs);
    }

    public static ToolResult fallback(String toolName, String fallbackMsg, long elapsedMs) {
        return new ToolResult()
                .setToolName(toolName)
                .setSuccess(false)
                .setFallback(true)
                .setSummary(fallbackMsg)
                .setElapsedMs(elapsedMs);
    }
}
