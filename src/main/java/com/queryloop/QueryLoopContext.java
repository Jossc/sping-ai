package com.queryloop;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.ai.document.Document;
import org.springframework.ai.chat.messages.Message;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Accessors(chain = true)
public class QueryLoopContext {

    private String userId;
    private String originalInput;
    private String cleanedInput;

    /** LLM 原始分类结果 */
    private IntentType intent = IntentType.GENERAL_CHAT;

    /** 置信度 0.0 ~ 1.0 */
    private double confidence = 0.0;

    /** 最终路由 (经会话裁决 + 权益裁决后) */
    private IntentType finalRoute = IntentType.GENERAL_CHAT;

    // ── 管线各阶段注入的数据 ──

    /** [PreProcessor] 用户权益画像 */
    private UserProfile userProfile = UserProfile.defaultFree("");

    /** [StateReader] 结构化会话状态 */
    private SessionState sessionState = new SessionState();

    /** [StateReader] 原始消息历史 */
    private List<Message> chatHistory = new ArrayList<>();

    /** [Planner] RAG 检索到的文档 */
    private List<Document> ragDocuments = new ArrayList<>();

    /** [Planner] 权益拦截原因 (越权时填写) */
    private String rejectReason = "";

    /** [Planner] 本轮 AI 响应 */
    private String response = "";

    private LocalDateTime timestamp = LocalDateTime.now();
    private Map<String, Object> metadata = new HashMap<>();

    // ── 便捷方法 ──

    public boolean isFirstSession() {
        return sessionState.isFirstSession();
    }

    public IntentType getLastIntent() {
        return sessionState.getLastIntent();
    }

    /** 当前用户是否被权益拦截 (finalRoute 降级了) */
    public boolean isDowngraded() {
        return intent != finalRoute && rejectReason != null && !rejectReason.isEmpty();
    }
}
