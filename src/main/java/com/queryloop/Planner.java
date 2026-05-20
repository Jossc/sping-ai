package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor.CHAT_MEMORY_CONVERSATION_ID_KEY;

@Slf4j
@Component
public class Planner {

    private final ChatClient chatClient;
    private final VectorStore vectorStore;
    private final ChatMemory chatMemory;
    private final ToolExecutor toolExecutor;
    private final ToolRegistry toolRegistry;

    private static final String RAG_SYSTEM_PROMPT = """
            你是由玄枢架构师开发的【护交付·智能风控助手】。
            你的说话风格必须：**专业、冷静、简练**。

            请结合【背景知识】来回答问题。

            规则：
            1. 遇到 P0 级高危事件，要在回答开头加三个红色警示符号：🚨🚨🚨。
            2. 如果背景知识里没有，直接回答"请联系人工坐席（电话 400-8888）"。
            """;

    public Planner(ChatClient.Builder builder,
                   VectorStore vectorStore,
                   ChatMemory chatMemory,
                   ToolExecutor toolExecutor,
                   ToolRegistry toolRegistry) {
        this.chatClient = builder.build();
        this.vectorStore = vectorStore;
        this.chatMemory = chatMemory;
        this.toolExecutor = toolExecutor;
        this.toolRegistry = toolRegistry;
    }

    public Flux<String> planAndExecute(QueryLoopContext ctx, String traceId) {
        IntentType route = ctx.getFinalRoute();

        if (route == IntentType.TOOL_CALL && !ctx.getUserProfile().canAccess(IntentType.TOOL_CALL)) {
            log.warn("[QL][{}] ▶ 3/5 Planner BLOCKED tier={}", traceId, ctx.getUserProfile().getTier());
            ctx.getSessionState().activateContext(IntentType.GENERAL_CHAT);
            return Flux.just(buildEntitlementRejection(ctx));
        }

        String input = truncate(ctx.getCleanedInput(), 60);
        log.info("[QL][{}] ▶ 3/5 Planner route={} userId={} tier={} input=\"{}\"",
                traceId, route, ctx.getUserId(), ctx.getUserProfile().getTier(), input);

        return switch (route) {
            case TOOL_CALL    -> executeToolCall(ctx, traceId);
            case RAG_SEARCH   -> executeRagSearch(ctx, traceId);
            case MEMORY_QUERY -> executeMemoryQuery(ctx, traceId);
            case GENERAL_CHAT -> executeGeneralChat(ctx, traceId);
        };
    }

    // ═══ TOOL_CALL — Phase 2: 集成 ToolExecutor ═══
    private Flux<String> executeToolCall(QueryLoopContext ctx, String traceId) {
        // 检查 SessionState 中是否有上一轮工具结果，支持追问分页
        ToolResult lastResult = ctx.getSessionState().getLastToolResult();
        boolean isPaginationFollowUp = lastResult != null
                && lastResult.getPagination() != null
                && lastResult.getPagination().isHasMore()
                && isFollowUpPhrasing(ctx.getCleanedInput());

        if (isPaginationFollowUp) {
            log.info("[QL][{}]    ↳ 检测到分页追问 → 复用上一工具 {} 的结果", traceId, lastResult.getToolName());
            ctx.getSessionState().activateContext(IntentType.TOOL_CALL);
            return Flux.just("上一查询还有更多结果，目前暂不支持自动翻页。请告知具体需求。");
        }

        log.info("[QL][{}]    ↳ 路由 → TOOL_CALL functions=[stockFunction,timeFunction,alarmService]",
                traceId);
        return chatClient.prompt()
                .user(ctx.getCleanedInput())
                .functions("stockFunction", "timeFunction", "alarmService")
                .stream()
                .content()
                .doOnComplete(() -> {
                    ctx.getSessionState().activateContext(IntentType.TOOL_CALL);
                    log.info("[QL][{}]    ↳ TOOL_CALL 完成 → activeContext=TOOL_CALL", traceId);
                })
                .doOnError(e ->
                        log.error("[QL][{}]    ↳ TOOL_CALL 异常 err={}", traceId, e.toString()));
    }

    /**
     * Phase 2: 执行已注册的业务工具（非 Function Calling 路径）
     */
    public ToolResult executeBusinessTool(String toolName, Map<String, Object> params) {
        return toolExecutor.execute(toolName, params);
    }

    // ═══ RAG_SEARCH ═══
    private Flux<String> executeRagSearch(QueryLoopContext ctx, String traceId) {
        long t0 = System.currentTimeMillis();
        List<Document> docs = vectorStore.similaritySearch(ctx.getCleanedInput());
        long vecMs = System.currentTimeMillis() - t0;
        ctx.setRagDocuments(docs);

        if (docs.isEmpty()) {
            log.warn("[QL][{}]    ↳ 路由 → RAG_SEARCH vectorMs={} retrieved=0", traceId, vecMs);
        } else {
            log.info("[QL][{}]    ↳ 路由 → RAG_SEARCH vectorMs={} retrieved={}", traceId, vecMs, docs.size());
        }

        String knowledge = docs.stream()
                .map(Document::getContent)
                .collect(Collectors.joining("\n\n"));

        String userPrompt = """
                【背景知识】：
                %s

                【用户问题】：%s
                """.formatted(knowledge, ctx.getCleanedInput());

        return chatClient.prompt()
                .system(RAG_SYSTEM_PROMPT)
                .user(userPrompt)
                .stream()
                .content()
                .doOnComplete(() ->
                        ctx.getSessionState().activateContext(IntentType.RAG_SEARCH))
                .doOnError(e ->
                        log.error("[QL][{}]    ↳ RAG_SEARCH 异常 err={}", traceId, e.toString()));
    }

    // ═══ MEMORY_QUERY ═══
    private Flux<String> executeMemoryQuery(QueryLoopContext ctx, String traceId) {
        log.info("[QL][{}]    ↳ 路由 → MEMORY_QUERY historySize={}", traceId, ctx.getChatHistory().size());
        return chatClient.prompt()
                .user(ctx.getCleanedInput())
                .advisors(new MessageChatMemoryAdvisor(chatMemory))
                .advisors(a -> a.param(CHAT_MEMORY_CONVERSATION_ID_KEY, ctx.getUserId()))
                .stream()
                .content()
                .doOnError(e ->
                        log.error("[QL][{}]    ↳ MEMORY_QUERY 异常 err={}", traceId, e.toString()));
    }

    // ═══ GENERAL_CHAT ═══
    private Flux<String> executeGeneralChat(QueryLoopContext ctx, String traceId) {
        String prompt = ctx.getCleanedInput();
        if (ctx.isDowngraded() && ctx.getIntent() == IntentType.RAG_SEARCH) {
            prompt = "[系统提示：用户权益不足以使用知识库检索，本条将以普通对话回答] " + prompt;
        }
        log.info("[QL][{}]    ↳ 路由 → GENERAL_CHAT downgraded={}", traceId, ctx.isDowngraded());
        return chatClient.prompt()
                .user(prompt)
                .stream()
                .content()
                .doOnComplete(() ->
                        ctx.getSessionState().activateContext(IntentType.GENERAL_CHAT))
                .doOnError(e ->
                        log.error("[QL][{}]    ↳ GENERAL_CHAT 异常 err={}", traceId, e.toString()));
    }

    // ═══ 辅助 ═══
    private String buildEntitlementRejection(QueryLoopContext ctx) {
        UserProfile p = ctx.getUserProfile();
        return """
                ⚠️ 权益不足

                您当前权益等级为 **%s**，可用功能：%s。
                工具调用需要 **PREMIUM** 及以上权益。
                如需升级，请联系管理员。
                """.formatted(p.getTier(), p.getAllowedIntents());
    }

    private boolean isFollowUpPhrasing(String input) {
        String[] p = {"还有", "继续", "然后", "下一页", "更多", "之前", "上次"};
        for (String s : p) if (input.contains(s)) return true;
        return input.length() <= 8;
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
