package com.queryloop.plan;

import com.queryloop.QueryLoopContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PlanReflector {

    private final ChatModel chatModel;

    private static final String REFLECT_PROMPT = """
            你是一个执行反思器。根据当前计划和已完成的步骤结果，判断是否需要调整计划。

            输出格式（严格 JSON）：
            {"action": "CONTINUE|REPLAN|ABORT", "reason": "判断理由"}

            当前计划：
            %s

            已完成步骤及结果：
            %s

            剩余步骤：
            %s

            判断标准：
            - CONTINUE：已完成的步骤结果足够支撑剩余步骤继续执行
            - REPLAN：需要调整剩余步骤（如发现新信息需要额外查询、或某步骤失败需要替代方案）
            - ABORT：无法继续（如关键步骤失败且无法补救、用户权益不足等）
            """;

    public PlanReflector(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    public ReflectionResult reflect(ExecutionPlan plan, QueryLoopContext ctx, String traceId) {
        // 已完成步骤摘要
        StringBuilder completed = new StringBuilder();
        for (PlanStep s : plan.getSteps()) {
            if (s.getStatus() == StepStatus.COMPLETED && s.getResult() != null) {
                completed.append(String.format("[Step%d:%s] %s\n",
                        s.getSeq(), s.getIntent(), truncate(s.getResult(), 200)));
            } else if (s.getStatus() == StepStatus.FAILED) {
                completed.append(String.format("[Step%d:FAILED]\n", s.getSeq()));
            }
        }

        // 剩余步骤
        StringBuilder remaining = new StringBuilder();
        for (PlanStep s : plan.getSteps()) {
            if (s.getStatus() == StepStatus.PENDING) {
                remaining.append(String.format("[Step%d:%s] %s\n",
                        s.getSeq(), s.getIntent(), s.getSubQuery()));
            }
        }

        if (remaining.isEmpty()) {
            return ReflectionResult.continue_("所有步骤已完成");
        }

        String prompt = String.format(REFLECT_PROMPT,
                plan.getPlanRationale(), completed.toString(), remaining.toString());

        try {
            String raw = chatModel.call(new Prompt(prompt))
                    .getResult().getOutput().getContent();
            log.info("[Reflect][{}] raw=\"{}\"", traceId, truncate(raw, 100));

            if (raw.contains("REPLAN")) {
                return ReflectionResult.replan(extractReason(raw), plan.getOriginalQuery());
            } else if (raw.contains("ABORT")) {
                return ReflectionResult.abort(extractReason(raw));
            }
            return ReflectionResult.continue_("执行正常");
        } catch (Exception e) {
            log.warn("[Reflect][{}] Reflection LLM 异常，默认 CONTINUE err={}", traceId, e.toString());
            return ReflectionResult.continue_("Reflection 调用异常，默认继续");
        }
    }

    private String extractReason(String raw) {
        return raw.length() > 120 ? raw.substring(0, 120) : raw;
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
