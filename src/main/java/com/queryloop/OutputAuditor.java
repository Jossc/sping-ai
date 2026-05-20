package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 输出安全审计器 —— 在 LLM 执行之后、返回用户之前执行。
 *
 * <h3>检测类型</h3>
 * <ul>
 *   <li><b>泄露检测</b>：识别 LLM 输出中是否包含 System Prompt 或内部信息</li>
 *   <li><b>敏感信息检测</b>：识别手机号、身份证号等 PII 泄露</li>
 *   <li><b>长度限制</b>：超出阈值自动截断</li>
 * </ul>
 *
 * <h3>在管线中的位置</h3>
 * <pre>
 *   ... → Planner (LLM 执行) → <b>OutputAuditor</b> → StateWriter → 用户响应
 * </pre>
 */
@Slf4j
@Component
public class OutputAuditor {

    // ═══════════════════════════════════════════════
    // 泄露检测模式
    // ═══════════════════════════════════════════════

    /** System Prompt 片段泄露 */
    private static final List<Pattern> PROMPT_LEAK_PATTERNS = List.of(
            Pattern.compile("你是(由)?.*(开发的|设计的|创建的).*(助手|机器人|AI)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(system\\s*prompt|系统提示词|系统指令).*[:：]", Pattern.CASE_INSENSITIVE),
            Pattern.compile("你的(角色|设定|规则|约束)是", Pattern.CASE_INSENSITIVE),
            Pattern.compile("玄枢架构师", Pattern.CASE_INSENSITIVE)
    );

    /** 敏感信息模式 */
    private static final Pattern PHONE_PATTERN =
            Pattern.compile("1[3-9]\\d{9}");
    private static final Pattern ID_CARD_PATTERN =
            Pattern.compile("\\d{17}[\\dXx]");
    private static final Pattern API_KEY_PATTERN =
            Pattern.compile("(sk-|api[_-]?key[=:])\\s*[A-Za-z0-9_\\-]{20,}", Pattern.CASE_INSENSITIVE);
    private static final Pattern IP_PATTERN =
            Pattern.compile("\\b(10\\.\\d{1,3}|172\\.(1[6-9]|2\\d|3[01])|192\\.168)\\.\\d{1,3}\\.\\d{1,3}\\b");

    // ═══════════════════════════════════════════════
    // 限制
    // ═══════════════════════════════════════════════

    /** 输出最大长度（字符） */
    private static final int MAX_OUTPUT_LENGTH = 16_384;

    // ═══════════════════════════════════════════════
    // 审计结果
    // ═══════════════════════════════════════════════

    public enum Severity {
        PASS,
        WARN,
        REDACT,
        BLOCK
    }

    public static class AuditResult {
        private final String auditedOutput;
        private final Severity severity;
        private final List<String> anomalies;
        private final boolean truncated;

        AuditResult(String auditedOutput, Severity severity, List<String> anomalies, boolean truncated) {
            this.auditedOutput = auditedOutput;
            this.severity = severity;
            this.anomalies = anomalies;
            this.truncated = truncated;
        }

        public static AuditResult pass(String output) {
            return new AuditResult(output, Severity.PASS, List.of(), false);
        }

        public static AuditResult redacted(String output, List<String> anomalies) {
            return new AuditResult(output, Severity.REDACT, anomalies, false);
        }

        public static AuditResult blocked(String output, List<String> anomalies) {
            return new AuditResult(output, Severity.BLOCK, anomalies, false);
        }

        public static AuditResult truncated(String output, int originalLen) {
            return new AuditResult(output, Severity.WARN,
                    List.of("输出超长截断: " + originalLen + " → " + output.length()), true);
        }

        public String getAuditedOutput() { return auditedOutput; }
        public Severity getSeverity() { return severity; }
        public List<String> getAnomalies() { return anomalies; }
        public boolean isTruncated() { return truncated; }
        public boolean isBlocked() { return severity == Severity.BLOCK; }
    }

    // ═══════════════════════════════════════════════
    // 审计入口
    // ═══════════════════════════════════════════════

    /**
     * 对 LLM 输出执行安全审计。
     *
     * @param response LLM 原始输出
     * @param userId   用户 ID
     * @param traceId  链路追踪 ID
     * @return 审计结果（可能被脱敏或截断）
     */
    public AuditResult audit(String response, String userId, String traceId) {
        if (response == null || response.isEmpty()) {
            return AuditResult.pass("");
        }

        List<String> anomalies = new java.util.ArrayList<>();

        // ── 1. Prompt 泄露检测 ──
        for (Pattern p : PROMPT_LEAK_PATTERNS) {
            java.util.regex.Matcher m = p.matcher(response);
            if (m.find()) {
                String matched = m.group();
                anomalies.add("Prompt泄露: " + truncate(matched, 60));
                log.warn("[Auditor][{}] ⚠️ Prompt 泄露检测命中 userId={} matched=\"{}\"",
                        traceId, userId, matched);
            }
        }

        // ── 2. 敏感信息检测 ──
        checkAndRedact(response, PHONE_PATTERN, "手机号", anomalies, traceId, userId);
        checkAndRedact(response, ID_CARD_PATTERN, "身份证号", anomalies, traceId, userId);
        checkAndRedact(response, API_KEY_PATTERN, "API Key", anomalies, traceId, userId);

        // 内部 IP 检测（非脱敏，仅告警）
        java.util.regex.Matcher ipMatcher = IP_PATTERN.matcher(response);
        if (ipMatcher.find()) {
            anomalies.add("内部IP泄露: " + ipMatcher.group());
            log.warn("[Auditor][{}] ⚠️ 内部 IP 泄露检测命中 userId={} ip={}",
                    traceId, userId, ipMatcher.group());
        }

        // ── 3. 严重泄露 → 阻断 ──
        if (!anomalies.isEmpty()) {
            boolean hasSevere = anomalies.stream().anyMatch(a ->
                    a.startsWith("Prompt泄露") || a.startsWith("API Key"));
            if (hasSevere) {
                log.error("[Auditor][{}] 🚫 输出阻断 userId={} anomalies={}",
                        traceId, userId, anomalies);
                return AuditResult.blocked(
                        "⚠️ 本条回复因安全审计未通过已被拦截。如有疑问请联系管理员。",
                        anomalies);
            }
        }

        // ── 4. 脱敏处理 ──
        String audited = response;
        if (!anomalies.isEmpty()) {
            audited = PHONE_PATTERN.matcher(audited).replaceAll("***");
            audited = ID_CARD_PATTERN.matcher(audited).replaceAll("***");
            audited = API_KEY_PATTERN.matcher(audited).replaceAll("***");
        }

        // ── 5. 长度限制 ──
        if (audited.length() > MAX_OUTPUT_LENGTH) {
            log.warn("[Auditor][{}] 输出过长 userId={} len={} → 截断至 {}",
                    traceId, userId, audited.length(), MAX_OUTPUT_LENGTH);
            audited = audited.substring(0, MAX_OUTPUT_LENGTH)
                    + "\n\n[响应过长已截断]";
            return AuditResult.truncated(audited, response.length());
        }

        if (!anomalies.isEmpty()) {
            log.warn("[Auditor][{}] ⚠️ 输出已脱敏 userId={} anomalies={}",
                    traceId, userId, anomalies);
            return AuditResult.redacted(audited, anomalies);
        }

        log.debug("[Auditor][{}] ✅ 审计通过 userId={}", traceId, userId);
        return AuditResult.pass(audited);
    }

    private void checkAndRedact(String text, Pattern pattern, String label,
                                 List<String> anomalies, String traceId, String userId) {
        java.util.regex.Matcher m = pattern.matcher(text);
        if (m.find()) {
            anomalies.add(label + "泄露: " + m.group());
            log.warn("[Auditor][{}] ⚠️ {} 泄露检测命中 userId={}",
                    traceId, label, userId);
        }
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
