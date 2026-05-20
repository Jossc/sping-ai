package com.queryloop.plan;

import com.queryloop.IntentType;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

@Data
@Accessors(chain = true)
public class PlanStep {

    private String stepId;
    private int seq;
    private IntentType intent = IntentType.GENERAL_CHAT;

    /** 已改写为可独立执行的完整问题 */
    private String subQuery;

    /** LLM 解释为什么需要这一步 */
    private String reasoning;

    /** 依赖的前序步骤序号 */
    private List<Integer> dependsOn = new ArrayList<>();

    private StepStatus status = StepStatus.PENDING;

    /** 本轮执行结果（完整文本） */
    private String result;

    /** 结构化工具结果 JSON（仅 TOOL_CALL） */
    private String toolResultJson;

    private long startedAt;
    private long completedAt;
}
