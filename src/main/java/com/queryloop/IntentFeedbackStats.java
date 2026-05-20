package com.queryloop;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * 按意图类型聚合的反馈统计
 *
 * 用于驱动置信度阈值自适应调整：
 *   当 correctedRate > 30% 时，该意图类型可信度降低
 *   当 adoptedRate > 90% 且样本 > 10 时，恢复默认阈值
 */
@Data
@Accessors(chain = true)
public class IntentFeedbackStats {

    /** 总分类次数 */
    private int totalCount = 0;

    /** 用户采纳次数 */
    private int adoptedCount = 0;

    /** 用户纠正次数 */
    private int correctedCount = 0;

    /** 用户忽略次数 */
    private int ignoredCount = 0;

    /** 最近 N 条详细记录（默认保留 10 条） */
    private List<IntentFeedbackRecord> recentRecords = new ArrayList<>();

    private static final int MAX_RECENT = 10;

    // ── 派生指标 ──

    /** 采纳率 = adopted / total */
    public double getAdoptionRate() {
        if (totalCount == 0) return 1.0;
        return (double) adoptedCount / totalCount;
    }

    /** 纠正率 = corrected / total */
    public double getCorrectionRate() {
        if (totalCount == 0) return 0.0;
        return (double) correctedCount / totalCount;
    }

    /** 忽略率 = ignored / total */
    public double getIgnoreRate() {
        if (totalCount == 0) return 0.0;
        return (double) ignoredCount / totalCount;
    }

    /**
     * 该类意图是否需要降低置信度
     * 条件：纠正率超过 30% 且样本 >= 3
     */
    public boolean needsConfidencePenalty() {
        return totalCount >= 3 && getCorrectionRate() > 0.30;
    }

    /**
     * 置信度惩罚系数 0.0 ~ 1.0
     * 纠正率越高惩罚越重，最低降到 0.60
     */
    public double getConfidencePenalty() {
        if (!needsConfidencePenalty()) return 1.0;
        // 纠正率 30%→0.90, 50%→0.75, 80%→0.60
        double penalty = Math.max(0.60, 1.0 - getCorrectionRate() * 0.5);
        return Math.round(penalty * 100.0) / 100.0;
    }

    // ── 记录方法 ──

    public void recordAdopted(IntentFeedbackRecord record) {
        totalCount++;
        adoptedCount++;
        appendRecord(record);
    }

    public void recordCorrected(IntentFeedbackRecord record) {
        totalCount++;
        correctedCount++;
        appendRecord(record);
    }

    public void recordIgnored(IntentFeedbackRecord record) {
        totalCount++;
        ignoredCount++;
        appendRecord(record);
    }

    private void appendRecord(IntentFeedbackRecord record) {
        recentRecords.add(record);
        if (recentRecords.size() > MAX_RECENT) {
            recentRecords.remove(0);
        }
    }

    /** 被纠正次数过多时给 InputGovernance 的提示语 */
    public String toPromptHint() {
        if (!needsConfidencePenalty()) return "";
        return String.format(
                "注意：近期 %s 类意图被纠正 %d/%d 次（纠正率 %.0f%%），请更严格地判断。",
                "该", correctedCount, totalCount, getCorrectionRate() * 100);
    }
}
