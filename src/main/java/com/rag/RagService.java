package com.rag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.TextReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.SimpleVectorStore;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

import static java.awt.SystemColor.info;

@Slf4j
@Service
public class RagService {
    private final ChatClient chatClient;
    private final VectorStore vectorStore;

    public static String systemPrompt = """
            你是由玄枢架构师开发的【护交付·智能风控助手】。
            你的说话风格必须：**专业、冷静、简练**。
            
            请结合【对话历史】和【背景知识】来回答问题。
            
            规则：
            1. 遇到 P0 级高危事件，要在回答开头加三个红色警示符号：🚨🚨🚨。
            2. 如果用户的问题是追问（例如“要通知家属吗？”），请务必**关联上一轮对话中的事件上下文**（如具体的房间号、事件类型）进行回答，不要只背诵文档。
            3. 如果背景知识里没有，直接回答“请联系人工坐席（电话 400-8888）”。
            """;

    // 🔥 1. 注入 ChatMemory
    public RagService(VectorStore vectorStore, ChatClient.Builder builder, ChatMemory chatMemory) {
        this.vectorStore = vectorStore;
        this.chatClient = builder
                // 🔥 2. 挂载记忆增强器
                // 这里的 defaultAdvisors 意味着每次对话都会自动带上记忆
                .defaultAdvisors(new MessageChatMemoryAdvisor(chatMemory))
                // 🔥 3. 挂载你的报警工具 (之前是在 prompt 里写的，这里可以全局挂载)
                .defaultFunctions("alarmService")
                .build();
    }

    @Value("classpath:nursing_sop.txt")
    Resource sopResource;

    // 2. 项目启动时，自动把文档读进去 (ETL 过程)
    @PostConstruct
    public void init() {
        // 读取文本
//        TextReader reader = new TextReader(sopResource);
//        List<Document> documents = reader.get();
//
//        TokenTextSplitter splitter = new TokenTextSplitter();
//        List<Document> splitDocs = splitter.apply(documents);
//                  vectorStore.add(splitDocs);

        TextReader reader = new TextReader(sopResource);
        List<Document> documents = reader.get();

        vectorStore.add(documents);
    }

    public Flux<String> chatWithRagWithFlux(String question, String userId) {
        log.info("chatWithRagWithFlux quer {}, userId {}", question, userId);
        List<Document> similarDocs = vectorStore.similaritySearch(question);
        String info = similarDocs.stream()
                .map(Document::getContent)
                .collect(Collectors.joining("\n\n"));
        String userPrompt = """
                【背景知识】：
                %s
                
                【用户问题】：%s
                """.formatted(info, question);

        // 2. 启动流式对话
        return chatClient.prompt()
                .system(systemPrompt) // 记得保持你之前的 system prompt (包含 P0 规则)
                .user(userPrompt)
                .advisors(a -> a.param(MessageChatMemoryAdvisor.CHAT_MEMORY_CONVERSATION_ID_KEY, userId))
                // 🔥 核心变化：由 call() 变为 stream()
                .stream()
                .content(); // 返回的是 Flux<String>
    }

    public String chatWithRag(String question, String userId) {
        List<Document> similarDocs = vectorStore.similaritySearch(question);
        // 🔥 加强版日志：看看到底捞到了啥？
        System.out.println("--------------------------------------------------");
        System.out.println("❓ 用户问题: " + question);
        System.out.println("📉 检索到的文档数量: " + similarDocs.size());
        String info = "";
        if (!similarDocs.isEmpty()) {
            info = similarDocs.get(0).getContent();
            System.out.println("📄 检索到的具体内容 (Top 1): \n" + info);
        } else {
            System.out.println("⚠️ 警告：没有检索到任何相关文档！背景知识为空！");
        }
        System.out.println("--------------------------------------------------");

        String userPrompt = """
                【背景知识】：
                %s
                
                【用户问题】：%s
                """.formatted(info, question);
        return chatClient.prompt()
                .system(systemPrompt)
                .user(userPrompt)
                .advisors(a ->
                        a.param(MessageChatMemoryAdvisor.CHAT_MEMORY_CONVERSATION_ID_KEY, userId))
                .call()
                .content();
    }
}
