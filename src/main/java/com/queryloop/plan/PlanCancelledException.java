package com.queryloop.plan;

/**
 * Plan 执行被取消时抛出的异常，用于协作式中断执行链路。
 */
public class PlanCancelledException extends RuntimeException {

    private final String planId;

    public PlanCancelledException(String planId) {
        super("Plan " + planId + " 已被取消");
        this.planId = planId;
    }

    public String getPlanId() {
        return planId;
    }
}
