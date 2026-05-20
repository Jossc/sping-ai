package com.queryloop.plan;

public enum StepCheckpointType {
    /** 步骤执行前保存 */
    BEFORE_EXECUTION,
    /** 步骤完成后保存 */
    AFTER_COMPLETION,
    /** 步骤失败后保存 */
    AFTER_FAILURE
}
