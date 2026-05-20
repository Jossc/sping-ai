package com.queryloop.plan;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Accessors(chain = true)
public class ExecutionPlan {

    private String planId;
    private String originalQuery;
    private String cleanedQuery;
    private List<PlanStep> steps = new ArrayList<>();
    private PlanStatus status = PlanStatus.PLANNING;
    private int currentStepIndex = -1;
    private double planConfidence;
    private String planRationale;
    private int replanCount = 0;

    private static final int MAX_REPLAN = 2;
    private LocalDateTime createdAt = LocalDateTime.now();

    public PlanStep getCurrentStep() {
        if (currentStepIndex < 0 || currentStepIndex >= steps.size()) return null;
        return steps.get(currentStepIndex);
    }

    public boolean hasNextStep() {
        return currentStepIndex + 1 < steps.size();
    }

    public boolean canReplan() {
        return replanCount < MAX_REPLAN;
    }

    /** 获取已完成步骤的结果摘要，供后续 step 注入上下文 */
    public String getCompletedContext() {
        StringBuilder sb = new StringBuilder();
        for (PlanStep step : steps) {
            if (step.getStatus() == StepStatus.COMPLETED && step.getResult() != null) {
                if (sb.length() > 0) sb.append("\n");
                sb.append("[步骤").append(step.getSeq()).append("结果] ").append(step.getResult());
            }
        }
        return sb.toString();
    }
}
