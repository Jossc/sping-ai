package com.queryloop.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.queryloop.IntentType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
public class PlanGenerator {

    private final ChatModel chatModel;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final String PLAN_SYSTEM_PROMPT = """
            你是一个任务规划器。分析用户输入，将其拆解为 1-4 个有序执行步骤。

            输出格式（严格 JSON，不要任何额外文字）：
            {
              "rationale": "整体规划理由（一句话）",
              "confidence": 0.85,
              "steps": [
                {
                  "seq": 0,
                  "intent": "TOOL_CALL",
                  "subQuery": "该步骤的具体查询内容（已改写为可独立执行的完整句子）",
                  "reasoning": "为什么需要这一步",
                  "dependsOn": []
                }
              ]
            }

            意图类型（intent）：
            - TOOL_CALL：需要调用业务工具（查订单、查物流、创建工单）
            - RAG_SEARCH：需要检索知识库中的规范文档
            - MEMORY_QUERY：需要回顾历史对话中的内容
            - GENERAL_CHAT：普通对话或综合已获得的信息给出建议

            规划原则：
            1. 先查事实（TOOL_CALL），再查规范（RAG_SEARCH），最后综合分析（GENERAL_CHAT）
            2. 每步 subQuery 必须是独立可执行的完整问题（包含具体参数）
            3. 如果上一步的结果会影响下一步的查询内容，标记 dependsOn
            4. 如果不确定，宁可少拆一步（confidence 降低）
            5. 简单问题（如纯闲聊、单次查询）只输出 1 个 step
            """;

    public PlanGenerator(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    public ExecutionPlan generate(String originalQuery, String userContext) {
        String traceId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[PlanGen][{}] 开始规划 query=\"{}\"", traceId, truncate(originalQuery, 80));

        long t0 = System.currentTimeMillis();
        String prompt = String.format("用户权益：%s\n用户输入：%s", userContext, originalQuery);

        String rawJson;
        try {
            rawJson = chatModel.call(new Prompt(PLAN_SYSTEM_PROMPT + "\n" + prompt))
                    .getResult().getOutput().getContent();
            log.info("PlanGen rawJson {}",rawJson);
        } catch (Exception e) {
            log.error("[PlanGen][{}] LLM 调用失败，降级为单步计划 err={}", traceId, e.toString());
            return fallbackSingleStep(originalQuery);
        }

        long elapsed = System.currentTimeMillis() - t0;
        log.info("[PlanGen][{}] 规划完成 {}ms raw=\"{}\"", traceId, elapsed, truncate(rawJson, 120));

        try {
            return parsePlan(rawJson, originalQuery);
        } catch (Exception e) {
            log.error("[PlanGen][{}] JSON 解析失败，降级为单步计划 err={}", traceId, e.toString());
            return fallbackSingleStep(originalQuery);
        }
    }

    private ExecutionPlan fallbackSingleStep(String query) {
        ExecutionPlan plan = new ExecutionPlan()
                .setPlanId(UUID.randomUUID().toString().substring(0, 8))
                .setOriginalQuery(query)
                .setCleanedQuery(query.strip())
                .setPlanRationale("无法拆解，作为单步对话处理")
                .setPlanConfidence(0.5);

        PlanStep step = new PlanStep()
                .setStepId(UUID.randomUUID().toString().substring(0, 8))
                .setSeq(0)
                .setIntent(IntentType.GENERAL_CHAT)
                .setSubQuery(query)
                .setReasoning("降级兜底：保持原始查询不变");
        plan.getSteps().add(step);
        plan.setStatus(PlanStatus.PLANNING);
        return plan;
    }

    private ExecutionPlan parsePlan(String rawJson, String originalQuery) {
        String json = extractJson(rawJson);
        try {
            PlanResponseDto dto = mapper.readValue(json, PlanResponseDto.class);

            ExecutionPlan plan = new ExecutionPlan()
                    .setPlanId(UUID.randomUUID().toString().substring(0, 8))
                    .setOriginalQuery(originalQuery)
                    .setCleanedQuery(originalQuery.strip())
                    .setPlanRationale(dto.rationale)
                    .setPlanConfidence(dto.confidence);

            for (PlanStepDto s : dto.steps) {
                IntentType intent;
                try {
                    intent = IntentType.valueOf(s.intent);
                } catch (IllegalArgumentException e) {
                    intent = IntentType.GENERAL_CHAT;
                }
                PlanStep step = new PlanStep()
                        .setStepId(UUID.randomUUID().toString().substring(0, 8))
                        .setSeq(s.seq)
                        .setIntent(intent)
                        .setSubQuery(s.subQuery)
                        .setReasoning(s.reasoning);
                if (s.dependsOn != null) step.setDependsOn(s.dependsOn);
                plan.getSteps().add(step);
            }

            if (plan.getSteps().isEmpty()) {
                PlanStep defaultStep = new PlanStep()
                        .setStepId(UUID.randomUUID().toString().substring(0, 8))
                        .setSeq(0)
                        .setIntent(IntentType.GENERAL_CHAT)
                        .setSubQuery(originalQuery)
                        .setReasoning("LLM 未返回步骤，使用默认单步");
                plan.getSteps().add(defaultStep);
            }

            return plan;

        } catch (Exception e) {
            throw new RuntimeException("Plan JSON parse failed", e);
        }
    }

    private String extractJson(String raw) {
        String s = raw.strip();
        if (s.startsWith("```")) {
            int start = s.indexOf("{");
            int end = s.lastIndexOf("}");
            if (start >= 0 && end > start) {
                s = s.substring(start, end + 1);
            }
        }
        return s;
    }

    private String truncate(String s, int max) {
        if (s == null) return "null";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    // ── JSON 映射 DTO ──
    static class PlanResponseDto {
        public String rationale;
        public double confidence;
        public List<PlanStepDto> steps;
    }

    static class PlanStepDto {
        public int seq;
        public String intent;
        public String subQuery;
        public String reasoning;
        public List<Integer> dependsOn;
    }
}
