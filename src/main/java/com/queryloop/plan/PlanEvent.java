package com.queryloop.plan;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@AllArgsConstructor
public class PlanEvent {

    private String type;
    private LocalDateTime timestamp;
    private Map<String, Object> payload;

    public static PlanEvent plan(ExecutionPlan plan) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("planId", plan.getPlanId());
        p.put("totalSteps", plan.getSteps().size());
        p.put("rationale", plan.getPlanRationale());
        p.put("confidence", plan.getPlanConfidence());
        p.put("steps", plan.getSteps().stream().map(s -> {
            Map<String, Object> sm = new LinkedHashMap<>();
            sm.put("seq", s.getSeq());
            sm.put("intent", s.getIntent().name());
            sm.put("subQuery", s.getSubQuery());
            sm.put("reasoning", s.getReasoning());
            return sm;
        }).toList());
        return new PlanEvent("plan", LocalDateTime.now(), p);
    }

    public static PlanEvent stepStart(int idx, PlanStep step) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("stepIndex", idx);
        p.put("intent", step.getIntent().name());
        p.put("subQuery", step.getSubQuery());
        return new PlanEvent("step_start", LocalDateTime.now(), p);
    }

    public static PlanEvent stepChunk(int idx, String chunk) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("stepIndex", idx);
        p.put("chunk", chunk);
        return new PlanEvent("step_chunk", LocalDateTime.now(), p);
    }

    public static PlanEvent stepComplete(int idx, PlanStep step) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("stepIndex", idx);
        p.put("result", step.getResult());
        p.put("elapsedMs", step.getCompletedAt() - step.getStartedAt());
        return new PlanEvent("step_complete", LocalDateTime.now(), p);
    }

    public static PlanEvent stepFailed(int idx, String error) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("stepIndex", idx);
        p.put("error", error);
        return new PlanEvent("step_failed", LocalDateTime.now(), p);
    }

    public static PlanEvent stepSkipped(int idx, String reason) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("stepIndex", idx);
        p.put("reason", reason);
        return new PlanEvent("step_skipped", LocalDateTime.now(), p);
    }

    public static PlanEvent replan(ExecutionPlan plan, String reason) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("reason", reason);
        p.put("totalSteps", plan.getSteps().size());
        p.put("remainingSteps", plan.getSteps().size() - plan.getCurrentStepIndex() - 1);
        return new PlanEvent("replan", LocalDateTime.now(), p);
    }

    public static PlanEvent complete(String summary) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("summary", summary);
        return new PlanEvent("complete", LocalDateTime.now(), p);
    }

    public static PlanEvent error(String message) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("message", message);
        return new PlanEvent("error", LocalDateTime.now(), p);
    }
}
