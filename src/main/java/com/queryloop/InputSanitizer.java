package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 输入安全过滤器 —— 在 InputGovernance 之前执行。
 *
 * <h3>检测类型</h3>
 * <ul>
 *   <li><b>注入检测</b>：识别试图覆盖 System Prompt 的注入模式</li>
 *   <li><b>越狱检测</b>：识别试图绕过安全约束的越狱模式</li>
 * </ul>
 *
 * <h3>在管线中的位置</h3>
 * <pre>
 *   PreProcessor → StateReader → <b>InputSanitizer</b> → InputGovernance → ...
 * </pre>
 */
@Slf4j
@Component
public class InputSanitizer {

    // ═══════════════════════════════════════════════
    // 注入检测模式
    // ═══════════════════════════════════════════════

    /** 直接覆盖 System Prompt 的指令 */
    private static final List<Pattern> INJECTION_PATTERNS = List.of(
            Pattern.compile("ignore\\s+(all\\s+)?(previous|above|prior|your)\\s+(instructions?|prompts?|rules?)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("forget\\s+(everything|all)\\s+(you|we)\\s+(said|discussed|talked)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(you\\s+are|act\\s+as|pretend\\s+to\\s+be|roleplay\\s+as)\\s+(now\\s+)?(a\\s+)?(different|new)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(system|你的)\\s*(prompt|提示词|设定|指令)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(忽略|忘记|无视)\\s*(所有|之前|上面|前面|一切)\\s*(的)?\\s*(指令|提示|对话|规则|设定)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(你现在是|从现在开始你是|假装你是|扮演)"),
            Pattern.compile("(输出|打印|显示|告诉我)\\s*(你的)?\\s*(system\\s*prompt|系统提示|系统指令|初始设定)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("```(system|prompt).*```", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)
    );

    // ═══════════════════════════════════════════════
    // 越狱检测模式
    // ═══════════════════════════════════════════════

    private static final List<Pattern> JAILBREAK_PATTERNS = List.of(
            Pattern.compile("\\bDAN\\b.*(mode|模式)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(jailbreak|越狱|破解)\\s*(mode|模式|指令|提示)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(开发者模式|developer\\s*mode)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(do\\s*anything\\s*now|没有任何限制|无限制模式)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(解除|取消|关闭)\\s*(所有|一切)?\\s*(限制|安全|约束|规则)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("你不需要遵守.*规则", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(I\\s*gnore|disregard)\\s+(all\\s+)?(safety|security|ethical|content)\\s+(guidelines|rules|restrictions)", Pattern.CASE_INSENSITIVE)
    );

    // ═══════════════════════════════════════════════
    // 风险等级
    // ═══════════════════════════════════════════════

    public enum RiskLevel {
        /** 安全 */
        NONE,
        /** 低风险：可疑但不过滤 */
        LOW,
        /** 中风险：部分匹配，降级处理 */
        MEDIUM,
        /** 高风险：明确注入/越狱，直接拒绝 */
        HIGH
    }

    // ═══════════════════════════════════════════════
    // 过滤结果
    // ═══════════════════════════════════════════════

    public static class SanitizeResult {
        private final String sanitizedInput;
        private final boolean blocked;
        private final RiskLevel riskLevel;
        private final String reason;

        SanitizeResult(String sanitizedInput, boolean blocked, RiskLevel riskLevel, String reason) {
            this.sanitizedInput = sanitizedInput;
            this.blocked = blocked;
            this.riskLevel = riskLevel;
            this.reason = reason;
        }

        public static SanitizeResult pass(String input) {
            return new SanitizeResult(input, false, RiskLevel.NONE, null);
        }

        public static SanitizeResult warn(String input, RiskLevel level, String reason) {
            return new SanitizeResult(input, false, level, reason);
        }

        public static SanitizeResult block(RiskLevel level, String reason) {
            return new SanitizeResult(null, true, level, reason);
        }

        public String getSanitizedInput() { return sanitizedInput; }
        public boolean isBlocked() { return blocked; }
        public RiskLevel getRiskLevel() { return riskLevel; }
        public String getReason() { return reason; }
    }

    // ═══════════════════════════════════════════════
    // 检测入口
    // ═══════════════════════════════════════════════

    /**
     * 对用户输入执行安全过滤。
     *
     * @param rawInput 原始用户输入
     * @param userId   用户 ID（用于审计日志）
     * @param traceId  链路追踪 ID
     * @return 过滤结果
     */
    public SanitizeResult sanitize(String rawInput, String userId, String traceId) {
        if (rawInput == null || rawInput.isBlank()) {
            return SanitizeResult.pass("");
        }

        String cleaned = rawInput.strip().replaceAll("\\s{2,}", " ");

        // ── 1. 注入检测 ──
        for (Pattern p : INJECTION_PATTERNS) {
            java.util.regex.Matcher m = p.matcher(cleaned);
            if (m.find()) {
                String matched = m.group();
                log.warn("[Sanitizer][{}] ⛔ 注入检测命中 userId={} pattern=\"{}\" matched=\"{}\" input=\"{}\"",
                        traceId, userId, p.pattern(), matched, truncate(cleaned, 80));
                return SanitizeResult.block(RiskLevel.HIGH,
                        "输入包含疑似注入模式: " + truncate(matched, 60));
            }
        }

        // ── 2. 越狱检测 ──
        for (Pattern p : JAILBREAK_PATTERNS) {
            java.util.regex.Matcher m = p.matcher(cleaned);
            if (m.find()) {
                String matched = m.group();
                log.warn("[Sanitizer][{}] ⛔ 越狱检测命中 userId={} pattern=\"{}\" matched=\"{}\" input=\"{}\"",
                        traceId, userId, p.pattern(), matched, truncate(cleaned, 80));
                return SanitizeResult.block(RiskLevel.HIGH,
                        "输入包含疑似越狱模式: " + truncate(matched, 60));
            }
        }

        // ── 3. 长度限制 ──
        if (cleaned.length() > 4000) {
            log.warn("[Sanitizer][{}] 输入过长 userId={} len={}", traceId, userId, cleaned.length());
            String truncated = cleaned.substring(0, 4000);
            return SanitizeResult.warn(truncated, RiskLevel.LOW,
                    "输入过长，已截断至 4000 字符");
        }

        log.debug("[Sanitizer][{}] ✅ 检测通过 userId={}", traceId, userId);
        return SanitizeResult.pass(cleaned);
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
